package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

public class LiteralLong implements LogicalExpr {
    private long n;

    public long getN() {
        return n;
    }

    public LiteralLong(long n) {
        this.n = n;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(Long.toString(n), ArrowTypes.Int64Type);
    }

    @Override
    public String toString() {
        return Long.toString(n);
    }
}