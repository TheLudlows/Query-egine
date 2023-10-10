package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.Field;

public abstract class AggregateExpr implements LogicalExpr {
    protected String name;
    protected LogicalExpr expr;

    public AggregateExpr(String name, LogicalExpr expr) {
        this.name = name;
        this.expr = expr;
    }

    public String getName() {
        return name;
    }

    public LogicalExpr getExpr() {
        return expr;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, expr.toField(input).getDataType());
    }

    @Override
    public String toString() {
        return name + "(" + expr + ")";
    }
}