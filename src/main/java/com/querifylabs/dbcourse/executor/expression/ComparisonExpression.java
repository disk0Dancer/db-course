package com.querifylabs.dbcourse.executor.expression;

import com.querifylabs.dbcourse.executor.DataPage;
import com.querifylabs.dbcourse.executor.ExecutionContext;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.sql.SqlKind;

public class ComparisonExpression implements ExpressionNode {
    private final SqlKind op;
    private final ExpressionNode left;
    private final ExpressionNode right;

    public ComparisonExpression(SqlKind op, ExpressionNode left, ExpressionNode right) {
        this.op = op;
        this.left = left;
        this.right = right;
    }

    @Override
    public ValueVector evaluate(ExecutionContext ctx, DataPage page) {
        ValueVector leftVec = left.evaluate(ctx, page);
        ValueVector rightVec = right.evaluate(ctx, page);

        try {
            int rowCount = page.getRowCount();
            BitVector result = new BitVector("cmp", ctx.getAllocator());
            result.allocateNew(rowCount);

            TimeStampMicroVector l = (TimeStampMicroVector) leftVec;
            TimeStampMicroVector r = (TimeStampMicroVector) rightVec;

            for (int i = 0; i < rowCount; i++) {
                if (l.isNull(i) || r.isNull(i)) {
                    result.setNull(i);
                } else {
                    long lVal = l.get(i);
                    long rVal = r.get(i);
                    boolean match = false;
                    if (op == SqlKind.GREATER_THAN_OR_EQUAL) {
                        match = lVal >= rVal;
                    } else if (op == SqlKind.LESS_THAN_OR_EQUAL) {
                        match = lVal <= rVal;
                    }
                    result.set(i, match ? 1 : 0);
                }
            }
            result.setValueCount(rowCount);
            return result;
        } finally {
            if (left.producesNewVector()) {
                leftVec.close();
            }
            if (right.producesNewVector()) {
                rightVec.close();
            }
        }
    }

    @Override
    public boolean producesNewVector() {
        return true;
    }
}

