package org.example.expressions;

public interface Accumulator {
    void accumulate(Object value);

    Object finalValue();
}

