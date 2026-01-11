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

            for (int i = 0; i < rowCount; i++) {
                if (leftVec.isNull(i) || rightVec.isNull(i)) {
                    result.setNull(i);
                } else {
                    long lVal = getLong(leftVec, i);
                    long rVal = getLong(rightVec, i);
                    boolean match = false;
                    switch (op) {
                        case GREATER_THAN_OR_EQUAL: match = lVal >= rVal; break;
                        case LESS_THAN_OR_EQUAL: match = lVal <= rVal; break;
                        case GREATER_THAN: match = lVal > rVal; break;
                        case LESS_THAN: match = lVal < rVal; break;
                        case EQUALS: match = lVal == rVal; break;
                        case NOT_EQUALS: match = lVal != rVal; break;
                        default: throw new UnsupportedOperationException("Unknown op: " + op);
                    }
                    result.set(i, match ? 1 : 0);
                }
            }
            result.setValueCount(rowCount);
            return result;
        } finally {
            if (left.producesNewVector()) leftVec.close();
            if (right.producesNewVector()) rightVec.close();
        }
    }

    private long getLong(ValueVector v, int index) {
        if (v instanceof TimeStampMicroVector) return ((TimeStampMicroVector) v).get(index);
        if (v instanceof org.apache.arrow.vector.TimeStampMilliVector) return ((org.apache.arrow.vector.TimeStampMilliVector) v).get(index);
        if (v instanceof org.apache.arrow.vector.BigIntVector) return ((org.apache.arrow.vector.BigIntVector) v).get(index);
        if (v instanceof org.apache.arrow.vector.IntVector) return ((org.apache.arrow.vector.IntVector) v).get(index);
        throw new UnsupportedOperationException("Vector type not supported in cmp: " + v.getClass().getName());
    }

    @Override
    public boolean producesNewVector() {
        return true;
    }
}
