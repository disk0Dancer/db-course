package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ProjectIntoTableScanRule extends RelOptRule {
    public static final ProjectIntoTableScanRule INSTANCE = new ProjectIntoTableScanRule();

    private ProjectIntoTableScanRule() {
        super(operand(PhysicalProject.class, operand(PhysicalTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        var project = (PhysicalProject) call.rel(0);
        var scan = (PhysicalTableScan) call.rel(1);

        Set<Integer> usedColumns = new LinkedHashSet<>();
        for (RexNode expr : project.getProjects()) {
            collectInputRefs(expr, usedColumns);
        }

        if (usedColumns.isEmpty() || usedColumns.size() == scan.getTable().getRowType().getFieldCount()) return;
        if (scan.getProjectedColumns() != null) return;

        List<Integer> projectedColumns = new ArrayList<>(usedColumns);

        PhysicalTableScan newScan = new PhysicalTableScan(
            scan.getCluster(), scan.getTraitSet(), scan.getHints(),
            scan.getTable(), projectedColumns, scan.getFilter()
        );

        Mappings.TargetMapping mapping = Mappings.target(projectedColumns, scan.getTable().getRowType().getFieldCount());

        List<RexNode> newProjects = new ArrayList<>();
        for (RexNode expr : project.getProjects()) {
            newProjects.add(expr.accept(new org.apache.calcite.rex.RexPermuteInputsShuttle(mapping)));
        }

        // check if all projects are just identity column refs
        boolean allIdentity = true;
        if (newProjects.size() == projectedColumns.size()) {
            for (int i = 0; i < newProjects.size(); i++) {
                RexNode p = newProjects.get(i);
                if (!(p instanceof RexInputRef) || ((RexInputRef) p).getIndex() != i) {
                    allIdentity = false;
                    break;
                }
            }
        } else {
            allIdentity = false;
        }

        if (allIdentity) {
            call.transformTo(newScan);
        } else {
            PhysicalProject newProject = new PhysicalProject(
                project.getCluster(), project.getTraitSet(), project.getHints(),
                newScan, newProjects, project.getRowType(), project.getVariablesSet()
            );
            call.transformTo(newProject);
        }
    }

    private void collectInputRefs(RexNode node, Set<Integer> columns) {
        if (node instanceof RexInputRef) {
            columns.add(((RexInputRef) node).getIndex());
        } else {
            node.accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    columns.add(inputRef.getIndex());
                    return null;
                }
            });
        }
    }
}

