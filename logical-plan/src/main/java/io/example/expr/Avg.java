package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing the AVG aggregate expression.
 */
public class Avg extends AggregateExpr {
    public Avg(LogicalExpr input) {
        super("AVG", input);
    }
}
