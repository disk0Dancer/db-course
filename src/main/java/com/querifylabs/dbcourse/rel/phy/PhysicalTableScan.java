package com.querifylabs.dbcourse.rel.phy;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class PhysicalTableScan extends TableScan implements PhysicalRel {
    private final List<Integer> projectedColumns;
    private final List<RexNode> filter;

    protected PhysicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
                                RelOptTable table) {
        this(cluster, traitSet, hints, table, null, null);
    }

    protected PhysicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
                                RelOptTable table, List<Integer> projectedColumns, List<RexNode> filter) {
        super(cluster, traitSet, hints, table);
        this.projectedColumns = projectedColumns;
        this.filter = filter;
    }

    public List<Integer> getProjectedColumns() {
        return projectedColumns;
    }

    public List<RexNode> getFilter() {
        return filter;
    }

    @Override
    public RelDataType deriveRowType() {
        if (projectedColumns == null) return super.deriveRowType();

        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        RelDataType fullRowType = super.deriveRowType();
        for (int colIndex : projectedColumns) {
            builder.add(fullRowType.getFieldList().get(colIndex));
        }
        return builder.build();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost cost = super.computeSelfCost(planner, mq);
        if (projectedColumns != null) {
            double factor = (double) projectedColumns.size() / table.getRowType().getFieldCount();
            cost = cost.multiplyBy(factor);
        }
        if (filter != null) cost = cost.multiplyBy(0.5);
        return cost;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (projectedColumns != null) pw.item("projected", projectedColumns);
        if (filter != null) pw.item("filter", filter);
        return pw;
    }
}
