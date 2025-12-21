package com.querifylabs.dbcourse.executor;

import com.querifylabs.dbcourse.rel.phy.PhysicalTableScan;
import com.querifylabs.dbcourse.schema.ParquetTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

/**
 * Converter that translates optimized node to an executable tree.
 */
public class PlanImplementor {
    public ExecutionNode implementPlan(ExecutionContext ctx, RelNode root) {
        if (root instanceof PhysicalTableScan) {
            PhysicalTableScan scan = (PhysicalTableScan) root;
            ParquetTable table = scan.getTable().unwrap(ParquetTable.class);
            return new ParquetScanNode(ctx, table, scan.getProjectedColumns());
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
            return super.visit(other);
        }

        private RelNode visitPhysicalScan(PhysicalTableScan scan) {
            ParquetTable table = scan.getTable().unwrap(ParquetTable.class);
            result = new ParquetScanNode(ctx, table, scan.getProjectedColumns());
            return scan;
        }
    }
}
