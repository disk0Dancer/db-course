package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class PhysicalFilter extends Filter implements PhysicalRel {
    protected PhysicalFilter(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexNode condition) {
        super(cluster, traits, hints, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PhysicalFilter(getCluster(), traitSet, hints, input, condition);
    }
}
