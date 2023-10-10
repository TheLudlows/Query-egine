package io.example.expr;

import io.example.LogicalExpr;

/**
 * Logical expression representing the MAX aggregate expression.
 */
public class Max extends AggregateExpr {
    public Max(LogicalExpr input) {
        super("MAX", input);
    }
}
