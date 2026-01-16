package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

public class FilterIntoTableScanRule extends RelOptRule {
    public static final FilterIntoTableScanRule INSTANCE = new FilterIntoTableScanRule();

    private FilterIntoTableScanRule() {
        super(operand(PhysicalFilter.class, operand(PhysicalTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        var filter = (PhysicalFilter) call.rel(0);
        var scan = (PhysicalTableScan) call.rel(1);

        List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
        List<RexNode> pushableFilters = new ArrayList<>();

        for (RexNode conj : conjunctions) {
            RexNode expanded = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, conj);
            if (isSimpleComparison(expanded)) {
                pushableFilters.add(expanded);
            }
        }

        if (pushableFilters.isEmpty()) return;

        if (scan.getFilter() != null) {
            boolean allPresent = true;
            for (RexNode pf : pushableFilters) {
                boolean found = false;
                for (RexNode sf : scan.getFilter()) {
                    if (sf.toString().equals(pf.toString())) { 
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    allPresent = false;
                    break;
                }
            }
            if (allPresent) return;
        }

        RexNode filterToPush = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), pushableFilters);

        RexNode remappedFilter = filterToPush;
        if (scan.getProjectedColumns() != null) {
            Mappings.TargetMapping inverseMapping = Mappings.target(
                scan.getProjectedColumns(), scan.getTable().getRowType().getFieldCount()
            ).inverse();
            remappedFilter = filterToPush.accept(new org.apache.calcite.rex.RexPermuteInputsShuttle(inverseMapping));
        }

        List<RexNode> newFilters = new ArrayList<>();
        if (scan.getFilter() != null) {
            newFilters.addAll(scan.getFilter());
        }

        List<RexNode> remappedConjunctions = RelOptUtil.conjunctions(remappedFilter);
        for (RexNode rc : remappedConjunctions) {
             boolean found = false;
             if (scan.getFilter() != null) {
                 for (RexNode sf : scan.getFilter()) {
                     if (sf.toString().equals(rc.toString())) {
                         found = true;
                         break;
                     }
                 }
             }
             if (!found) {
                 newFilters.add(rc);
             }
        }

        if (newFilters.size() == (scan.getFilter() == null ? 0 : scan.getFilter().size())) {
            
            return;
        }

        PhysicalTableScan newScan = new PhysicalTableScan(
            scan.getCluster(), scan.getTraitSet(), scan.getHints(),
            scan.getTable(), scan.getProjectedColumns(), newFilters
        );

        
        PhysicalFilter newFilterRel = (PhysicalFilter) filter.copy(filter.getTraitSet(), newScan, filter.getCondition());
        call.transformTo(newFilterRel);
    }

    private boolean isSimpleComparison(RexNode node) {
        if (!(node instanceof RexCall)) return false;
        RexCall c = (RexCall) node;
        SqlKind kind = c.getKind();
        if (kind == SqlKind.EQUALS || kind == SqlKind.LESS_THAN ||
            kind == SqlKind.GREATER_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL ||
            kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.NOT_EQUALS) {
            if (c.getOperands().size() == 2) {
                for (RexNode op : c.getOperands()) {
                    if (isInputRefOrCast(op)) return true;
                }
            }
        }
        return false;
    }

    private boolean isInputRefOrCast(RexNode op) {
        if (op instanceof RexInputRef) return true;
        if (op instanceof RexCall && op.getKind() == SqlKind.CAST) {
            return ((RexCall) op).getOperands().get(0) instanceof RexInputRef;
        }
        return false;
    }
}

