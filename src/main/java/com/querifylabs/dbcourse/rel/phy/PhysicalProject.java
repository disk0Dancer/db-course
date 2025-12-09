package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

public class PhysicalProject extends Project implements PhysicalRel {
    protected PhysicalProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType,
            Set<CorrelationId> variableSet) {
        super(cluster, traits, hints, input, projects, rowType, variableSet);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PhysicalProject(getCluster(), traitSet, hints, input, projects, rowType, getVariablesSet());
    }
}
