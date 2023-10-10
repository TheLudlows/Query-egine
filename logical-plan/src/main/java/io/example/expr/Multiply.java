package io.example.expr;

import io.example.LogicalExpr;

public class Multiply extends MathExpr {
    public Multiply(LogicalExpr l, LogicalExpr r) {
        super("mult", "*", l, r);
    }

}
