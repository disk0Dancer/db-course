package com.querifylabs.dbcourse.executor.expression;

import com.querifylabs.dbcourse.executor.DataPage;
import com.querifylabs.dbcourse.executor.ExecutionContext;
import org.apache.arrow.vector.ValueVector;

public record InputRefExpression(int index) implements ExpressionNode {

    @Override
    public ValueVector evaluate(ExecutionContext ctx, DataPage page) {
        return page.getColumns().get(index);
    }

    @Override
    public boolean producesNewVector() {
        return false;
    }
}

