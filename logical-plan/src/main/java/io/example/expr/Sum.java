package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing the SUM aggregate expression.
 */
public class Sum extends AggregateExpr {
    public Sum(LogicalExpr input) {
        super("SUM", input);
    }
}
