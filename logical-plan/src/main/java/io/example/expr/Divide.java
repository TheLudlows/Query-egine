package io.example.expr;

import io.example.LogicalExpr;

public class Divide extends MathExpr {
    public Divide(LogicalExpr l, LogicalExpr r) {
        super("div", "/", l, r);
    }

}
