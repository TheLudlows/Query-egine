package org.example.expressions;

import static org.example.expressions.Common.toBool;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.ArrowFieldVector;
import org.example.ArrowTypes;
import org.example.ColumnVector;
import org.example.FieldVectorFactory;
import org.example.RecordBatch;

public abstract class BooleanExpression implements Expression {
    private final Expression l;
    private final Expression r;

    public BooleanExpression(Expression l, Expression r) {
        this.l = l;
        this.r = r;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        ColumnVector ll = l.evaluate(input);
        ColumnVector rr = r.evaluate(input);
        if (ll.size() != rr.size()) {
            throw new IllegalStateException("Cannot compare values of different size: " + ll.size() + " != " + rr.size());
        }
        if (ll.getType() != rr.getType()) {
            throw new IllegalStateException("Cannot compare values of different type: " + ll.getType() + " != " + rr.getType());
        }
        return compare(ll, rr);
    }

    private ColumnVector compare(ColumnVector l, ColumnVector r) {
        BitVector v = new BitVector("v", new RootAllocator(Long.MAX_VALUE));
        v.allocateNew();
        for (int i = 0; i < l.size(); i++) {
            boolean value = evaluate(l.getValue(i), r.getValue(i), l.getType());
            v.set(i, value ? 1 : 0);
        }
        v.setValueCount(l.size());
        return new ArrowFieldVector(v);
    }


    protected abstract boolean evaluate(Object l, Object r, ArrowType arrowType);
}

