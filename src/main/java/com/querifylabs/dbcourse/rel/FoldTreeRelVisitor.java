package com.querifylabs.dbcourse.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

public class FoldTreeRelVisitor extends RelShuttleImpl {
    @Override
    public RelNode visit(LogicalProject project) {
        var visited = (LogicalProject) super.visit(project);

        var rexVisitor = new FoldConstantsRexVisitor(visited.getCluster().getRexBuilder());
        List<RexNode> foldedProjects = visited.getProjects().stream()
            .map(expr -> expr.accept(rexVisitor))
            .collect(Collectors.toList());

        return LogicalProject.create(visited.getInput(), List.of(), foldedProjects, visited.getRowType().getFieldNames());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        var visited = (LogicalFilter) super.visit(filter);
        var rexVisitor = new FoldConstantsRexVisitor(visited.getCluster().getRexBuilder());
        RexNode foldedCondition = visited.getCondition().accept(rexVisitor);
        return visited.copy(visited.getTraitSet(), visited.getInput(), foldedCondition);
    }
}
