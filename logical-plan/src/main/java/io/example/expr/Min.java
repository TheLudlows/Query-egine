package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing the MIN aggregate expression.
 */
public class Min extends AggregateExpr {
    public Min(LogicalExpr input) {
        super("MIN", input);
    }
}
