package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.Field;

public class Add extends MathExpr {
    public Add(LogicalExpr l, LogicalExpr r) {
        super("add", "+", l, r);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(getName(), l.toField(input).getDataType());
    }
}
