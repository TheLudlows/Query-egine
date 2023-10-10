package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

public class LiteralFloat implements LogicalExpr {
    private float n;

    public LiteralFloat(float n) {
        this.n = n;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(Float.toString(n), ArrowTypes.FloatType);
    }

    @Override
    public String toString() {
        return Float.toString(n);
    }
}