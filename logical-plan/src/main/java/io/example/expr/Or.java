package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing a logical OR
 */
public class Or extends BooleanBinaryExpr {
    public Or(LogicalExpr l, LogicalExpr r) {
        super("or", "OR", l, r);
    }
}
