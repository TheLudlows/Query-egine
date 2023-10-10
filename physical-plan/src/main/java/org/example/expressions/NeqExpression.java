package org.example.expressions;

import static org.example.ArrowTypes.*;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class NeqExpression extends BooleanExpression {

    public NeqExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    protected boolean evaluate(Object l, Object r, ArrowType arrowType) {
        if(arrowType.equals(Int8Type)) {
            return !l.equals(r);
        } else if(arrowType.equals(Int16Type)) {
            return !l.equals(r);
        }else if(arrowType.equals(Int32Type)) {
            return !l.equals(r);
        }else if(arrowType.equals(Int64Type)) {
            return !l.equals(r);
        }else if(arrowType.equals(FloatType)) {
            return !l.equals(r);
        }else if(arrowType.equals(DoubleType)) {
            return !l.equals(r);
        }else if(arrowType.equals(StringType)) {
            return !l.equals(r);
        }else if(arrowType.equals(BooleanType)) {
            return !l.equals(r);
        } else {
            throw new IllegalStateException("Unsupported data type in comparison expression: " + arrowType);
        }
    }
}
