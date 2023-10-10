package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.Field;

public abstract class MathExpr extends BinaryExpr {
    private String name;

    public MathExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        super(name, op, l, r);
        this.name = name;
    }

    public Field toField(LogicalPlan input){
        return new Field(name, l.toField(input).getDataType());
    }

    public String getName() {
        return name;
    }
}
