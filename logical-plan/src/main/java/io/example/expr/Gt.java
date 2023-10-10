package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing a greater than (`>`) comparison
 */
public class Gt extends BooleanBinaryExpr {
    public Gt(LogicalExpr l, LogicalExpr r) {
        super("gt", ">", l, r);
    }
}
