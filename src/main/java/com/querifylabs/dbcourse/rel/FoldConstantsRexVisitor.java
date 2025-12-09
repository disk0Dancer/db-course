package com.querifylabs.dbcourse.rel;

import com.querifylabs.dbcourse.sql.SqlBase64DecodeFunction;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.avatica.util.ByteString;

import java.util.Base64;

public class FoldConstantsRexVisitor extends RexShuttle {
    private final RexBuilder rexBuilder;

    public FoldConstantsRexVisitor(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        call = (RexCall) super.visitCall(call);

        if (call.getOperator() instanceof SqlBase64DecodeFunction) {
            if (call.getOperands().size() == 1) {
                RexNode operand = call.getOperands().get(0);
                if (operand instanceof RexLiteral) {
                    RexLiteral lit = (RexLiteral) operand;
                    String val = null;
                    Object v = lit.getValue3();
                    if (v instanceof org.apache.calcite.util.NlsString) {
                        val = ((org.apache.calcite.util.NlsString) v).getValue();
                    } else if (v instanceof String) {
                        val = (String) v;
                    }
                    if (val != null) {
                        try {
                            byte[] decoded = Base64.getDecoder().decode(val);
                            return rexBuilder.makeBinaryLiteral(new ByteString(decoded));
                        } catch (Exception e) {
                            return rexBuilder.makeNullLiteral(call.getType());
                        }
                    }
                }
            }
        }
        return call;
    }
}
