package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.List;

public class PhysicalProjectRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG =
        Config.INSTANCE
            .withConversion(
                LogicalProject.class,
                project -> true,
                Convention.NONE,
                PhysicalConvention.INSTANCE,
                PhysicalProjectRule.class.getSimpleName())
            .withRuleFactory(PhysicalProjectRule::new);

    protected PhysicalProjectRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        var project = (LogicalProject) rel;
        return new PhysicalProject(
            project.getCluster(),
            project.getTraitSet().replace(PhysicalConvention.INSTANCE),
            List.of(),
            convert(project.getInput(), project.getInput().getTraitSet().replace(PhysicalConvention.INSTANCE)),
            project.getProjects(),
            project.getRowType(),
            project.getVariablesSet());
    }
}

