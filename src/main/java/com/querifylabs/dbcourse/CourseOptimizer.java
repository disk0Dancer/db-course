package com.querifylabs.dbcourse;

import com.querifylabs.dbcourse.rel.FoldTreeRelVisitor;
import com.querifylabs.dbcourse.rel.phy.FilterIntoTableScanRule;
import com.querifylabs.dbcourse.rel.phy.PhysicalConvention;
import com.querifylabs.dbcourse.rel.phy.ProjectIntoTableScanRule;
import com.querifylabs.dbcourse.rel.phy.ToPhysicalRules;
import com.querifylabs.dbcourse.schema.ParquetSchema;
import com.querifylabs.dbcourse.sql.ExtendedSqlOperatorTable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.io.StringReader;
import java.util.List;

public class CourseOptimizer {
    protected RelDataTypeFactory dataTypeFactory;

    protected RelOptCluster relOptCluster;
    protected SqlValidator validator;
    protected SqlToRelConverter sqlToRelConverter;
    protected VolcanoPlanner volcanoPlanner;

    public CourseOptimizer(String rootPath, boolean enableScanPushdown) {
        dataTypeFactory = new SqlTypeFactoryImpl(new CustomDataTypeSystem());
        var catalogReader = new CalciteCatalogReader(
                CalciteSchema.createRootSchema(false, true, "", new ParquetSchema(rootPath, dataTypeFactory)),
                List.of(),
                dataTypeFactory,
                CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false"));
        var operatorTable = SqlOperatorTables.chain(
                ExtendedSqlOperatorTable.instance(), SqlStdOperatorTable.instance());
        validator = SqlValidatorUtil.newValidator(
                operatorTable, catalogReader, dataTypeFactory, SqlValidator.Config.DEFAULT);

        volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        for (var rule : ToPhysicalRules.ALL) {
            volcanoPlanner.addRule(rule);
        }

        if (enableScanPushdown) {
            volcanoPlanner.addRule(ProjectIntoTableScanRule.INSTANCE);
            volcanoPlanner.addRule(FilterIntoTableScanRule.INSTANCE);
        }

        relOptCluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(dataTypeFactory));

        sqlToRelConverter = new SqlToRelConverter(
                null, validator, catalogReader, relOptCluster, StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config().withTrimUnusedFields(true));
    }

    public RelRoot convert(String sql) {
        var parsed = parseSql(sql);
        var validated = validator.validate(parsed);
        var relRoot = sqlToRelConverter.convertQuery(validated, false, true);
        var trimmedRel = sqlToRelConverter.trimUnusedFields(true, relRoot.rel);
        return relRoot.withRel(trimmedRel);
    }

    public RelRoot optimize(RelRoot unoptimized) {
        var program = getLogicalProgram();

        var optimized = program.run(volcanoPlanner,
                unoptimized.rel,
                relOptCluster.traitSet(),
                List.of(),
                List.of());

        return unoptimized.withRel(optimized);
    }

    public RelRoot optimizePhysical(RelRoot logical) {
        return logical.withRel(
                getPhysicalProgram().run(
                        volcanoPlanner,
                        logical.rel,
                        logical.rel.getTraitSet().replace(PhysicalConvention.INSTANCE),
                        List.of(),
                        List.of())
        );
    }

    protected SqlNode parseSql(String sql) throws ParserException {
        var parser = new SqlParserImpl(new StringReader(sql));
        parser.setQuotedCasing(Casing.UNCHANGED);
        parser.setUnquotedCasing(Casing.UNCHANGED);
        parser.setIdentifierMaxLength(255);
        parser.setConformance(SqlConformanceEnum.DEFAULT);

        try {
            return parser.parseSqlStmtEof();
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    protected Program getLogicalProgram() {
        return Programs.sequence(
                foldConstantsProgram()
        );
    }

    protected Program getPhysicalProgram() {
        return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
            planner.setRoot(rel);
            final RelNode rootRel =
                    rel.getTraitSet().equals(requiredOutputTraits)
                            ? rel
                            : planner.changeTraits(rel, requiredOutputTraits);

            planner.setRoot(rootRel);
            return planner.findBestExp();
        };
    }

    protected Program foldConstantsProgram() {
        return (planner, rel, requiredOutputTraits, materializations, lattices) -> rel.accept(new FoldTreeRelVisitor());
    }
}
