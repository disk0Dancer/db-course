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

        if (scan.getFilter() != null) return;

        List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
        List<RexNode> pushableFilters = new ArrayList<>();

        for (RexNode conj : conjunctions) {
            RexNode expanded = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, conj);
            if (isSimpleComparison(expanded)) pushableFilters.add(expanded);
        }

        if (pushableFilters.isEmpty()) return;

        RexNode filterToPush = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), pushableFilters);

        RexNode remappedFilter = filterToPush;
        if (scan.getProjectedColumns() != null) {
            Mappings.TargetMapping inverseMapping = Mappings.target(
                scan.getProjectedColumns(), scan.getTable().getRowType().getFieldCount()
            ).inverse();
            remappedFilter = filterToPush.accept(new org.apache.calcite.rex.RexPermuteInputsShuttle(inverseMapping));
        }

        PhysicalTableScan newScan = new PhysicalTableScan(
            scan.getCluster(), scan.getTraitSet(), scan.getHints(),
            scan.getTable(), scan.getProjectedColumns(), List.of(remappedFilter)
        );

        PhysicalFilter newFilter = (PhysicalFilter) filter.copy(filter.getTraitSet(), newScan, filter.getCondition());
        call.transformTo(newFilter);
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
                    if (op instanceof RexInputRef) return true;
                }
            }
        }
        return false;
    }
}

