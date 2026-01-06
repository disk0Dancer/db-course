package com.querifylabs.dbcourse.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class FoldTreeRelVisitor extends RelShuttleImpl {
    @Override
    public RelNode visit(LogicalProject project) {
        RelNode newInput = project.getInput().accept(this);

        var rexVisitor = new FoldConstantsRexVisitor(project.getCluster().getRexBuilder());
        List<RexNode> foldedProjects = project.getProjects().stream()
            .map(expr -> expr.accept(rexVisitor))
            .collect(Collectors.toList());

        // rm unused
        if (newInput instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) newInput;
            if (filter.getInput() instanceof LogicalTableScan) {
                LogicalTableScan scan = (LogicalTableScan) filter.getInput();

                Map<Integer, Integer> indexMap = new TreeMap<>();
                int[] counter = {0};

                project.getProjects().forEach(expr -> expr.accept(new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        indexMap.computeIfAbsent(inputRef.getIndex(), idx -> counter[0]++);
                        return super.visitInputRef(inputRef);
                    }
                }));

                filter.getCondition().accept(new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        indexMap.computeIfAbsent(inputRef.getIndex(), idx -> counter[0]++);
                        return super.visitInputRef(inputRef);
                    }
                });

                if (!indexMap.isEmpty() && indexMap.size() < scan.getRowType().getFieldCount()) {
                    var orderedIndexes = new ArrayList<>(indexMap.keySet());
                    var trimExprs = orderedIndexes.stream()
                        .map(idx -> (RexNode) new RexInputRef(idx, scan.getRowType().getFieldList().get(idx).getType()))
                        .collect(Collectors.toList());
                    List<String> fieldNames = orderedIndexes.stream()
                        .map(i -> scan.getRowType().getFieldList().get(i).getName())
                        .collect(Collectors.toList());

                    var trimmedScan = LogicalProject.create(scan, List.of(), trimExprs, fieldNames);
                    RexShuttle remapShuttle = getRexShuttle(project, indexMap);
                    RexNode remappedCondition = filter.getCondition().accept(remapShuttle);
                    LogicalFilter newFilter = filter.copy(filter.getTraitSet(), trimmedScan, remappedCondition);

                    foldedProjects = foldedProjects.stream()
                        .map(expr -> expr.accept(remapShuttle))
                        .collect(Collectors.toList());

                    return LogicalProject.create(newFilter, List.of(), foldedProjects, project.getRowType().getFieldNames());
                }
            }
        }

        return LogicalProject.create(newInput, List.of(), foldedProjects, project.getRowType().getFieldNames());
    }

    private static RexShuttle getRexShuttle(LogicalProject project, Map<Integer, Integer> indexMap) {
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RexShuttle remapShuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                Integer newIndex = indexMap.get(inputRef.getIndex());
                if (newIndex != null) {
                    return rexBuilder.makeInputRef(inputRef.getType(), newIndex);
                }
                return super.visitInputRef(inputRef);
            }
        };
        return remapShuttle;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        var visited = (LogicalFilter) super.visit(filter);
        var rexVisitor = new FoldConstantsRexVisitor(visited.getCluster().getRexBuilder());
        RexNode newCondition = visited.getCondition().accept(rexVisitor);
        return visited.copy(visited.getTraitSet(), visited.getInput(), newCondition);
    }
}
