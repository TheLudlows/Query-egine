package org.example.expressions;

import static org.example.ArrowTypes.BooleanType;
import static org.example.ArrowTypes.DoubleType;
import static org.example.ArrowTypes.FloatType;
import static org.example.ArrowTypes.Int16Type;
import static org.example.ArrowTypes.Int32Type;
import static org.example.ArrowTypes.Int64Type;
import static org.example.ArrowTypes.Int8Type;
import static org.example.ArrowTypes.StringType;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class EqExpression extends BooleanExpression {

    public EqExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    protected boolean evaluate(Object l, Object r, ArrowType arrowType) {
        if(arrowType.equals(Int8Type)) {
            return l.equals(r);
        } else if(arrowType.equals(Int16Type)) {
            return l.equals(r);
        }else if(arrowType.equals(Int32Type)) {
            return l.equals(r);
        }else if(arrowType.equals(Int64Type)) {
            return l.equals(r);
        }else if(arrowType.equals(FloatType)) {
            return l.equals(r);
        }else if(arrowType.equals(DoubleType)) {
            return l.equals(r);
        }else if(arrowType.equals(StringType)) {
            return l.equals(r);
        }else if(arrowType.equals(BooleanType)) {
            return l.equals(r);
        } else {
            throw new IllegalStateException("Unsupported data type in comparison expression: " + arrowType);
        }
    }
    @Override
    public String toString() {
        return "AndExpression: " + l + " = " + r;
    }
}
