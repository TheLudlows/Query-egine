package org.example.expressions;

import static org.example.ArrowTypes.DoubleType;
import static org.example.ArrowTypes.FloatType;
import static org.example.ArrowTypes.Int16Type;
import static org.example.ArrowTypes.Int32Type;
import static org.example.ArrowTypes.Int64Type;
import static org.example.ArrowTypes.Int8Type;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class SubtractExpression extends MathExpression {
    public SubtractExpression(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public Object evaluate(Object l, Object r, ArrowType arrowType) {
        if (arrowType.equals(Int8Type)) {
            return (byte) l - (byte) r;
        } else if (arrowType.equals(Int16Type)) {
            return (short) l - (short) r;
        } else if (arrowType.equals(Int32Type)) {
            return (int) l - (int) r;
        } else if (arrowType.equals(Int64Type)) {
            return (long) l - (long) r;
        } else if (arrowType.equals(FloatType)) {
            return (float) l - (float) r;
        } else if (arrowType.equals(DoubleType)) {
            return (double) l - (double) r;
        }
        throw new IllegalStateException("Unsupported data type in math expression: " + arrowType);
    }

    @Override
    public String toString() {
        return l.toString() + "-" + r.toString();
    }
}
