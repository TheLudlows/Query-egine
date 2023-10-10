package org.example.expressions;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.ArrowVectorBuilder;
import org.example.ColumnVector;
import org.example.FieldVectorFactory;

public abstract class MathExpression extends BinaryExpression {
    public MathExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    public ColumnVector evaluate(ColumnVector l, ColumnVector r) {
        FieldVector fieldVector = FieldVectorFactory.create(l.getType(), l.size());
        ArrowVectorBuilder builder = new ArrowVectorBuilder(fieldVector);
        for (int i = 0; i < l.size(); i++) {
            Object value = evaluate(l.getValue(i), r.getValue(i), l.getType());
            builder.set(i, value);
        }
        builder.setValueCount(l.size());
        return builder.build();
    }

    public abstract Object evaluate(Object l, Object r, ArrowType arrowType);
}