package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

/**
 * Logical expression representing the COUNT aggregate expression.
 */
public class Count extends AggregateExpr {
    public Count(LogicalExpr input) {
        super("COUNT", input);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field("COUNT", ArrowTypes.Int32Type);
    }

    @Override
    public String toString() {
        return "COUNT(" + expr + ")";
    }
}
