package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

public class LiteralDouble implements LogicalExpr {
    private double n;

    public double getN() {
        return n;
    }

    public LiteralDouble(double n) {
        this.n = n;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(Double.toString(n), ArrowTypes.DoubleType);
    }

    @Override
    public String toString() {
        return Double.toString(n);
    }
}