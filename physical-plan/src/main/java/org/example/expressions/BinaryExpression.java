package org.example.expressions;

import org.example.ColumnVector;
import org.example.RecordBatch;

/**
 * For binary expressions we need to evaluate the left and right input expressions and then evaluate
 * the specific binary operator against those input values, so we can use this base class to
 * simplify the implementation for each operator.
 */
public abstract class BinaryExpression implements Expression {
    protected final Expression l;
    protected final Expression r;

    public BinaryExpression(Expression l, Expression r) {
        this.l = l;
        this.r = r;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        ColumnVector ll = l.evaluate(input);
        ColumnVector rr = r.evaluate(input);
        assert ll.size() == rr.size();
        if (ll.getType() != rr.getType()) {
            throw new IllegalStateException("Binary expression operands do not have the same type: " + ll.getType() + " != " + rr.getType());
        }
        return evaluate(ll, rr);
    }

    public abstract ColumnVector evaluate(ColumnVector l, ColumnVector r);
}