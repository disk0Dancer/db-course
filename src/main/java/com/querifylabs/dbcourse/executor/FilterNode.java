package com.querifylabs.dbcourse.executor;

import com.querifylabs.dbcourse.executor.expression.ExpressionNode;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

public class FilterNode extends BaseExecutionNode {
    private final ExecutionNode input;
    private final ExpressionNode condition;

    public FilterNode(ExecutionContext executionContext, ExecutionNode input, ExpressionNode condition) {
        super(executionContext);
        this.input = input;
        this.condition = condition;
    }

    @Override
    public DataPage getNextPage() throws ExecutorException {
        while (true) {
            DataPage page = input.getNextPage();
            if (page == null) {
                return null;
            }

            ValueVector result = null;
            try {
                result = condition.evaluate(executionContext, page);
                if (!(result instanceof BitVector selection)) {
                    throw new ExecutorException("Filter condition must return boolean");
                }

                DataPage filtered = page.select(executionContext, selection);
                if (filtered.getRowCount() > 0) {
                    return filtered;
                }
                filtered.close();

            } finally {
                if (result != null && condition.producesNewVector()) {
                    result.close();
                }
                page.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        input.close();
    }
}

