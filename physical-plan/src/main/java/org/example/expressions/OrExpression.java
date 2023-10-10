package org.example.expressions;

import static org.example.expressions.Common.toBool;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class OrExpression extends BooleanExpression {
    public OrExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    protected boolean evaluate(Object l, Object r, ArrowType arrowType) {
        return toBool(l) || toBool(r);
    }
}
