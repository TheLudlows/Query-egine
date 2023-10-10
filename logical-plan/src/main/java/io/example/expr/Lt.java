package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing a less than (`<`) comparison
 */
public class Lt extends BooleanBinaryExpr {
    public Lt(LogicalExpr l, LogicalExpr r) {
        super("lt", "<", l, r);
    }
}
