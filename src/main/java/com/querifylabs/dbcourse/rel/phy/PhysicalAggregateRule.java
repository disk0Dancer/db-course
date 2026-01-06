package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

import java.util.List;

public class PhysicalAggregateRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG =
        Config.INSTANCE
            .withConversion(
                LogicalAggregate.class,
                agg -> true,
                Convention.NONE,
                PhysicalConvention.INSTANCE,
                PhysicalAggregateRule.class.getSimpleName())
            .withRuleFactory(PhysicalAggregateRule::new);

    protected PhysicalAggregateRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        var agg = (LogicalAggregate) rel;
        return new PhysicalAggregate(
            agg.getCluster(),
            agg.getTraitSet().replace(PhysicalConvention.INSTANCE),
            List.of(),
            convert(agg.getInput(), agg.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE)),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());
    }
}

