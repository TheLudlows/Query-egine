package org.example.expressions;

import static org.example.ArrowTypes.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.expressions.Expression;
import org.example.expressions.MathExpression;

public class DivideExpression extends MathExpression {
    public DivideExpression(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public Object evaluate(Object l, Object r, ArrowType arrowType) {
        if (arrowType.equals(Int8Type)) {
            return (byte) l / (byte) r;
        } else if (arrowType.equals(Int16Type)) {
            return (short) l / (short) r;
        } else if (arrowType.equals(Int32Type)) {
            return (int) l / (int) r;
        } else if (arrowType.equals(Int64Type)) {
            return (long) l / (long) r;
        } else if (arrowType.equals(FloatType)) {
            return (float) l / (float) r;
        } else if (arrowType.equals(DoubleType)) {
            return (double) l / (double) r;
        }
        throw new IllegalStateException("Unsupported data type in math expression: " + arrowType);
    }

    @Override
    public String toString() {
        return l.toString() + "/" + r.toString();
    }
}