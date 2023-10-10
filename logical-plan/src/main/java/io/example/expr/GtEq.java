package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing a greater than or equals (`>=`) comparison
 */
public class GtEq extends BooleanBinaryExpr {
    public GtEq(LogicalExpr l, LogicalExpr r) {
        super("gteq", ">=", l, r);
    }
}
