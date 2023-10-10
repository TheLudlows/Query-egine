package org.example.expressions;

import java.lang.UnsupportedOperationException;

public class SumExpression implements AggregateExpression {

    private Expression expr;

    public SumExpression(Expression expr) {
        this.expr = expr;
    }

    @Override
    public Expression inputExpression() {
        return expr;
    }

    @Override
    public Accumulator createAccumulator() {
        return new SumAccumulator();
    }

    @Override
    public String toString() {
        return "SUM(" + expr + ")";
    }

    public static class SumAccumulator implements Accumulator {

        private Object value;

        @Override
        public void accumulate(Object value) {
            if (value != null) {
                if (this.value == null) {
                    this.value = value;
                } else {
                    Object currentValue = this.value;
                    if (currentValue instanceof Byte) {
                        this.value = (Byte) currentValue + (Byte) value;
                    } else if (currentValue instanceof Short) {
                        this.value = (Short) currentValue + (Short) value;
                    } else if (currentValue instanceof Integer) {
                        this.value = (Integer) currentValue + (Integer) value;
                    } else if (currentValue instanceof Long) {
                        this.value = (Long) currentValue + (Long) value;
                    } else if (currentValue instanceof Float) {
                        this.value = (Float) currentValue + (Float) value;
                    } else if (currentValue instanceof Double) {
                        this.value = (Double) currentValue + (Double) value;
                    } else {
                        throw new UnsupportedOperationException(
                            "SUM is not implemented for type: " + value.getClass().getName());
                    }
                }
            }
        }

        @Override
        public Object finalValue() {
            return value;
        }
    }
}

