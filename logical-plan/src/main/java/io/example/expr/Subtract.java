package io.example.expr;

import io.example.LogicalExpr;

public class Subtract extends MathExpr {
    public Subtract(LogicalExpr l, LogicalExpr r) {
        super("subtract", "-", l, r);
    }
}
