package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.List;

public class PhysicalTableScanRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG =
        Config.INSTANCE
            .withConversion(
                LogicalTableScan.class,
                scan -> true,
                Convention.NONE,
                PhysicalConvention.INSTANCE,
                PhysicalTableScanRule.class.getSimpleName())
            .withRuleFactory(PhysicalTableScanRule::new);

    protected PhysicalTableScanRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        var scan = (LogicalTableScan) rel;
        return new PhysicalTableScan(
            scan.getCluster(),
            scan.getTraitSet().replace(PhysicalConvention.INSTANCE),
            List.of(),
            scan.getTable());
    }
}

