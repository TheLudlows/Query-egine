package org.example.expressions;

public interface AggregateExpression {
    Expression inputExpression();
    Accumulator createAccumulator();
}