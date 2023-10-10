package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing an inequality (`!=`) comparison
 */
public class Neq extends BooleanBinaryExpr {
    public Neq(LogicalExpr l, LogicalExpr r) {
        super("neq", "!=", l, r);
    }
}
