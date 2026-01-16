package com.querifylabs.dbcourse.executor.aggregation;

public interface Accumulator {
    void add(Object value);
    Object getResult();
}