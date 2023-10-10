package io.example.expr;

import io.example.LogicalExpr;

public class And extends BooleanBinaryExpr {
    public And(LogicalExpr l, LogicalExpr r) {
        super("and", "AND", l, r);
    }
}