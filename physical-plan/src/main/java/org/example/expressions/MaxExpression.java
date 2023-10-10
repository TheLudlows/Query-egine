package org.example.expressions;

import java.lang.UnsupportedOperationException;

public class MaxExpression implements AggregateExpression {

    private Expression expr;

    public MaxExpression(Expression expr) {
        this.expr = expr;
    }

    @Override
    public Expression inputExpression() {
        return expr;
    }

    @Override
    public Accumulator createAccumulator() {
        return new MaxAccumulator();
    }

    @Override
    public String toString() {
        return "MAX(" + expr + ")";
    }

    public static class MaxAccumulator implements Accumulator {

        private Object value;

        @Override
        public void accumulate(Object value) {
            if (value != null) {
                if (this.value == null) {
                    this.value = value;
                } else {
                    boolean isMax;
                    if (value instanceof Byte) {
                        isMax = (Byte) value > (Byte) this.value;
                    } else if (value instanceof Short) {
                        isMax = (Short) value > (Short) this.value;
                    } else if (value instanceof Integer) {
                        isMax = (Integer) value > (Integer) this.value;
                    } else if (value instanceof Long) {
                        isMax = (Long) value > (Long) this.value;
                    } else if (value instanceof Float) {
                        isMax = (Float) value > (Float) this.value;
                    } else if (value instanceof Double) {
                        isMax = (Double) value > (Double) this.value;
                    } else if (value instanceof String) {
                        isMax = ((String) value).compareTo((String) this.value) > 0;
                    } else {
                        throw new UnsupportedOperationException(
                            "MAX is not implemented for data type: " + value.getClass().getName());
                    }
                    if (isMax) {
                        this.value = value;
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

