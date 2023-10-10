package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

public abstract class BooleanBinaryExpr extends BinaryExpr {

    public BooleanBinaryExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        super(name, op, l, r);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, ArrowTypes.BooleanType);
    }
}