package com.querifylabs.dbcourse.executor.expression;

import com.querifylabs.dbcourse.executor.DataPage;
import com.querifylabs.dbcourse.executor.ExecutionContext;
import org.apache.arrow.vector.ValueVector;

public interface ExpressionNode {
    ValueVector evaluate(ExecutionContext ctx, DataPage page);
    boolean producesNewVector();
}

