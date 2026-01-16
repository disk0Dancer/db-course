package com.querifylabs.dbcourse.executor;

import com.querifylabs.dbcourse.executor.expression.ExpressionNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

public class ProjectNode extends BaseExecutionNode {
    private final ExecutionNode input;
    private final List<ExpressionNode> projects;

    public ProjectNode(ExecutionContext executionContext, ExecutionNode input, List<ExpressionNode> projects) {
        super(executionContext);
        this.input = input;
        this.projects = projects;
    }

    @Override
    public DataPage getNextPage() throws ExecutorException {
        DataPage page = input.getNextPage();
        if (page == null) return null;

        List<FieldVector> newVectors = new ArrayList<>();
        try {
            for (ExpressionNode expr : projects) {
                ValueVector v = expr.evaluate(executionContext, page);
                if (v instanceof FieldVector) {
                    if (expr.producesNewVector()) {
                        newVectors.add((FieldVector) v);
                    } else {
                        TransferPair tp = v.getTransferPair(executionContext.getAllocator());
                        tp.transfer();
                        newVectors.add((FieldVector) tp.getTo());
                    }
                } else {
                    throw new ExecutorException("Expression result is not a FieldVector");
                }
            }
            page.close();

            return new DataPage(newVectors);
        } catch (Exception e) {
            page.close();
            for (FieldVector v : newVectors) {
                v.close();
            }
            throw new ExecutorException("Project failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        input.close();
    }
}