package com.querifylabs.dbcourse.executor.expression;

import com.querifylabs.dbcourse.executor.DataPage;
import com.querifylabs.dbcourse.executor.ExecutionContext;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;

import java.time.Instant;
import java.util.Calendar;

public record LiteralExpression(RexLiteral literal) implements ExpressionNode {

    @Override
    public ValueVector evaluate(ExecutionContext ctx, DataPage page) {
        int rowCount = page.getRowCount();
        // Assuming timestamp for now as per task
        TimeStampMicroVector vector = new TimeStampMicroVector("literal", ctx.getAllocator());
        vector.allocateNew(rowCount);

        long value = 0;
        Object val = literal.getValue();

        switch (val) {
            case TimestampString timestampString -> value = timestampString.getMillisSinceEpoch() * 1000;
            case Long l -> value = l;
            case Calendar calendar -> value = calendar.getTimeInMillis() * 1000;
            case NlsString nlsString -> {
                try {
                    String s = nlsString.getValue();
                    s = s.replace(" ", "T") + "Z";
                    value = Instant.parse(s).toEpochMilli() * 1000;
                } catch (Exception e) {}
            }
            case null, default -> {
                try {
                    assert val != null;
                    value = Long.parseLong(val.toString());
                } catch (Exception e) {}
            }
        }

        for (int i = 0; i < rowCount; i++) {
            vector.set(i, value);
        }
        vector.setValueCount(rowCount);
        return vector;
    }

    @Override
    public boolean producesNewVector() {
        return true;
    }
}

