package org.example.expressions;

import static org.example.ArrowTypes.DoubleType;
import static org.example.ArrowTypes.FloatType;
import static org.example.ArrowTypes.Int16Type;
import static org.example.ArrowTypes.Int32Type;
import static org.example.ArrowTypes.Int64Type;
import static org.example.ArrowTypes.Int8Type;
import static org.example.ArrowTypes.StringType;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class LtExpression extends BooleanExpression {

    public LtExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    protected boolean evaluate(Object l, Object r, ArrowType arrowType) {
        if (arrowType.equals(Int8Type)) {
            return (Byte) l < (Byte) r;
        } else if (arrowType.equals(Int16Type)) {
            return (Short) l < (Short) r;
        } else if (arrowType.equals(Int32Type)) {
            return (Integer) l < (Integer) r;
        } else if (arrowType.equals(Int64Type)) {
            return (Long) l < (Long) r;
        } else if (arrowType.equals(FloatType)) {
            return (Float) l < (Float) r;
        } else if (arrowType.equals(DoubleType)) {
            return (Double) l < (Double) r;
        } else if (arrowType.equals(StringType)) {
            return Common.toString(l).compareTo(Common.toString(r)) < 0;
        } else {
            throw new IllegalStateException("Unsupported data type in comparison expression: " + arrowType);
        }
    }
    @Override
    public String toString() {
        return "AndExpression: " + l + " < " + r;
    }
}
