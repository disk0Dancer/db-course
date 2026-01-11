package com.querifylabs.dbcourse.executor;

import com.querifylabs.dbcourse.executor.expression.ExpressionCompiler;
import com.querifylabs.dbcourse.executor.expression.ExpressionNode;
import com.querifylabs.dbcourse.rel.phy.PhysicalFilter;
import com.querifylabs.dbcourse.rel.phy.PhysicalTableScan;
import com.querifylabs.dbcourse.schema.ParquetTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

/**
 * Converter that translates optimized node to an executable tree.
 */
public class PlanImplementor {
    public ExecutionNode implementPlan(ExecutionContext ctx, RelNode root) {
        if (root instanceof PhysicalTableScan scan) {
            assert scan.getTable() != null;
            ParquetTable table = scan.getTable().unwrap(ParquetTable.class);
            return new ParquetScanNode(ctx, table, scan.getProjectedColumns(), scan.getFilter());
        }
        ImplementationVisitor visitor = new ImplementationVisitor(ctx);
        root.accept(visitor);
        return visitor.result;
    }

    private static class ImplementationVisitor extends RelShuttleImpl {
        private final ExecutionContext ctx;
        private ExecutionNode result;

        public ImplementationVisitor(ExecutionContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.core.TableScan scan) {
            if (scan instanceof PhysicalTableScan) {
                return visitPhysicalScan((PhysicalTableScan) scan);
            }
            return super.visit(scan);
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof PhysicalTableScan) {
                return visitPhysicalScan((PhysicalTableScan) other);
            }
            if (other instanceof PhysicalFilter) {
                return visitPhysicalFilter((PhysicalFilter) other);
            }
            return super.visit(other);
        }

        private RelNode visitPhysicalScan(PhysicalTableScan scan) {
            assert scan.getTable() != null;
            ParquetTable table = scan.getTable().unwrap(ParquetTable.class);
            List<RexNode> filters = scan.getFilter();
            result = new ParquetScanNode(ctx, table, scan.getProjectedColumns(), filters);
            return scan;
        }

        private RelNode visitPhysicalFilter(PhysicalFilter filter) {
            super.visit(filter);
            ExecutionNode input = result;
            RexNode condition = filter.getCondition();
            RexNode expanded = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, condition);
            ExpressionNode expr = expanded.accept(new ExpressionCompiler());
            result = new FilterNode(ctx, input, expr);
            return filter;
        }
    }
}
