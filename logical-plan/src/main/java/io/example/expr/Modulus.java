package io.example.expr;

import io.example.LogicalExpr;

public class Modulus extends MathExpr {
    public Modulus(LogicalExpr l, LogicalExpr r) {
        super("mod", "%", l, r);
    }

}
