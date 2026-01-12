package com.querifylabs.dbcourse.executor.aggregation;

public class SumAccumulator implements Accumulator {
    private Long longSum = null;
    private Double doubleSum = null;
    private boolean isDouble = false;

    @Override
    public void add(Object value) {
        if (value == null) return;

        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            if (isDouble) {
                doubleSum = (doubleSum == null ? 0.0 : doubleSum) + ((Number) value).doubleValue();
            } else {
                longSum = (longSum == null ? 0L : longSum) + ((Number) value).longValue();
            }
        } else if (value instanceof Double || value instanceof Float) {
            if (!isDouble) {
                isDouble = true;
                doubleSum = (longSum == null ? 0.0 : longSum.doubleValue());
                longSum = null;
            }
            doubleSum = (doubleSum == null ? 0.0 : doubleSum) + ((Number) value).doubleValue();
        }
    }

    @Override
    public Object getResult() {
        if (isDouble) return doubleSum;
        return longSum;
    }
}

