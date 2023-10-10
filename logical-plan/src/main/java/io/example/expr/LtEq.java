package io.example.expr;

import io.example.LogicalExpr;

public class LtEq extends BooleanBinaryExpr {
    public LtEq(LogicalExpr l, LogicalExpr r) {
        super("lteq", "<=", l, r);
    }
}