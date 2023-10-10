package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

public class Not extends UnaryExpr {
    public Not(LogicalExpr expr) {
        super("not", "NOT", expr);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, ArrowTypes.BooleanType);
    }
}
