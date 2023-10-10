package org.example.expressions;

import java.lang.UnsupportedOperationException;

public class MinExpression implements AggregateExpression {

    private Expression expr;

    public MinExpression(Expression expr) {
        this.expr = expr;
    }

    @Override
    public Expression inputExpression() {
        return expr;
    }

    @Override
    public Accumulator createAccumulator() {
        return new MinAccumulator();
    }

    @Override
    public String toString() {
        return "MIN(" + expr + ")";
    }

    public static class MinAccumulator implements Accumulator {

        private Object value;

        @Override
        public void accumulate(Object value) {
            if (value != null) {
                if (this.value == null) {
                    this.value = value;
                } else {
                    boolean isMin;
                    if (value instanceof Byte) {
                        isMin = (Byte) value < (Byte) this.value;
                    } else if (value instanceof Short) {
                        isMin = (Short) value < (Short) this.value;
                    } else if (value instanceof Integer) {
                        isMin = (Integer) value < (Integer) this.value;
                    } else if (value instanceof Long) {
                        isMin = (Long) value < (Long) this.value;
                    } else if (value instanceof Float) {
                        isMin = (Float) value < (Float) this.value;
                    } else if (value instanceof Double) {
                        isMin = (Double) value < (Double) this.value;
                    } else if (value instanceof String) {
                        throw new UnsupportedOperationException(
                            "MIN is not implemented for String yet: " + (String) value);
                    } else {
                        throw new UnsupportedOperationException(
                            "MIN is not implemented for data type: " + value.getClass().getName());
                    }
                    if (isMin) {
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

