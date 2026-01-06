package com.querifylabs.dbcourse.executor.expression;

import com.querifylabs.dbcourse.executor.DataPage;
import com.querifylabs.dbcourse.executor.ExecutionContext;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

import java.util.List;

public record AndExpression(List<ExpressionNode> operands) implements ExpressionNode {

    @Override
    public ValueVector evaluate(ExecutionContext ctx, DataPage page) {
        if (operands.isEmpty()) {
            return null;
        }

        ValueVector current = operands.getFirst().evaluate(ctx, page);
        boolean currentIsNew = operands.getFirst().producesNewVector();

        for (int i = 1; i < operands.size(); i++) {
            ExpressionNode nextOp = operands.get(i);
            ValueVector next = nextOp.evaluate(ctx, page);
            boolean nextIsNew = nextOp.producesNewVector();

            try {
                BitVector l = (BitVector) current;
                BitVector r = (BitVector) next;
                BitVector result = new BitVector("and", ctx.getAllocator());
                result.allocateNew(page.getRowCount());

                for (int j = 0; j < page.getRowCount(); j++) {
                    if (l.isNull(j) || r.isNull(j)) {
                        result.setNull(j);
                    } else {
                        result.set(j, (l.get(j) == 1 && r.get(j) == 1) ? 1 : 0);
                    }
                }
                result.setValueCount(page.getRowCount());

                if (currentIsNew) {
                    current.close();
                }
                current = result;
                currentIsNew = true;

            } finally {
                if (nextIsNew) {
                    next.close();
                }
            }
        }

        return current;
    }

    @Override
    public boolean producesNewVector() {
        return true;
    }
}
