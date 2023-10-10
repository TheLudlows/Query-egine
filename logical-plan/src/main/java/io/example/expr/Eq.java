package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing an equality (`=`) comparison
 */
public class Eq extends BooleanBinaryExpr {
    public Eq(LogicalExpr l, LogicalExpr r) {
        super("eq", "=", l, r);
    }
}
