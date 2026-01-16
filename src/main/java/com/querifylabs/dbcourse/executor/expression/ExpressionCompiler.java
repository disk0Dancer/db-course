package com.querifylabs.dbcourse.executor.expression;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

public class ExpressionCompiler extends RexVisitorImpl<ExpressionNode> {
    public ExpressionCompiler() {
        super(true);
    }

    @Override
    public ExpressionNode visitInputRef(RexInputRef inputRef) {
        return new InputRefExpression(inputRef.getIndex());
    }

    @Override
    public ExpressionNode visitLiteral(RexLiteral literal) {
        return new LiteralExpression(literal);
    }

    @Override
    public ExpressionNode visitCall(RexCall call) {
        if (call.getKind() == SqlKind.AND) {
            List<ExpressionNode> operands = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                operands.add(operand.accept(this));
            }
            return new AndExpression(operands);
        } else if (call.getKind() == SqlKind.LESS_THAN_OR_EQUAL || call.getKind() == SqlKind.GREATER_THAN_OR_EQUAL
                || call.getKind() == SqlKind.LESS_THAN || call.getKind() == SqlKind.GREATER_THAN
                || call.getKind() == SqlKind.EQUALS || call.getKind() == SqlKind.NOT_EQUALS) {
            return new ComparisonExpression(
                call.getKind(),
                call.getOperands().get(0).accept(this),
                call.getOperands().get(1).accept(this)
            );
        }
        throw new UnsupportedOperationException("Op not supported: " + call.getKind());
    }
}

