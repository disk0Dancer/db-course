package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.RelOptRule;

import java.util.List;

public class ToPhysicalRules {
    public static final PhysicalFilterRule FILTER_RULE =
        PhysicalFilterRule.DEFAULT_CONFIG.toRule(PhysicalFilterRule.class);

    public static final PhysicalTableScanRule TABLE_SCAN_RULE =
        PhysicalTableScanRule.DEFAULT_CONFIG.toRule(PhysicalTableScanRule.class);

    public static final PhysicalProjectRule PROJECT_RULE =
        PhysicalProjectRule.DEFAULT_CONFIG.toRule(PhysicalProjectRule.class);

    public static final PhysicalAggregateRule AGGREGATE_RULE =
        PhysicalAggregateRule.DEFAULT_CONFIG.toRule(PhysicalAggregateRule.class);

    public static final List<RelOptRule> ALL = List.of(
        FILTER_RULE,
        TABLE_SCAN_RULE,
        PROJECT_RULE,
        AGGREGATE_RULE
    );
}
