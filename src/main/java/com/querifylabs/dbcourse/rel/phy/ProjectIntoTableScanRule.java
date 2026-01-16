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

        java.util.Map<Integer, Integer> scanOutputToBase = new java.util.HashMap<>();
        List<Integer> currentProjection = scan.getProjectedColumns();

        int scanFieldCount = scan.getRowType().getFieldCount();
        for (int i = 0; i < scanFieldCount; i++) {
            if (currentProjection == null) {
                scanOutputToBase.put(i, i);
            } else {
                scanOutputToBase.put(i, currentProjection.get(i));
            }
        }

        
        Set<Integer> neededBaseCols = new java.util.TreeSet<>();

        for (RexNode expr : project.getProjects()) {
            collectInputRefs(expr, neededBaseCols, scanOutputToBase);
        }

        
        boolean changed = false;
        List<Integer> newProjectedColumns = new ArrayList<>(neededBaseCols);

        if (currentProjection == null) {
            if (newProjectedColumns.size() < scan.getTable().getRowType().getFieldCount()) {
                changed = true;
            }
        } else {
            if (!newProjectedColumns.equals(currentProjection)) {
                changed = true;
            }
        }

        if (!changed) {
            
            
            return;
        }

        PhysicalTableScan newScan = new PhysicalTableScan(
            scan.getCluster(), scan.getTraitSet(), scan.getHints(),
            scan.getTable(), newProjectedColumns, scan.getFilter()
        );

        
        java.util.Map<Integer, Integer> baseToNewScan = new java.util.HashMap<>();
        for (int i = 0; i < newProjectedColumns.size(); i++) {
            baseToNewScan.put(newProjectedColumns.get(i), i);
        }

        
        List<RexNode> newProjects = new ArrayList<>();
        org.apache.calcite.rex.RexShuttle shuttle = new org.apache.calcite.rex.RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                
                Integer baseIdx = scanOutputToBase.get(inputRef.getIndex());
                
                Integer newScanIdx = baseToNewScan.get(baseIdx);
                return new RexInputRef(newScanIdx, inputRef.getType());
            }
        };

        for (RexNode expr : project.getProjects()) {
            newProjects.add(expr.accept(shuttle));
        }

        
        boolean allIdentity = true;
        if (newProjects.size() == newProjectedColumns.size()) {
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

    private void collectInputRefs(RexNode node, Set<Integer> columns, java.util.Map<Integer, Integer> mapping) {
        if (node instanceof RexInputRef) {
            columns.add(mapping.get(((RexInputRef) node).getIndex()));
        } else {
            node.accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    columns.add(mapping.get(inputRef.getIndex()));
                    return null;
                }
            });
        }
    }
}

