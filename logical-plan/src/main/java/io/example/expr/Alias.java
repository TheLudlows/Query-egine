package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.Field;

public class Alias implements LogicalExpr {
    private LogicalExpr expr;
    private String alias;

    public LogicalExpr getExpr() {
        return expr;
    }

    public String getAlias() {
        return alias;
    }

    public Alias(LogicalExpr expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(alias, expr.toField(input).getDataType());
    }

    @Override
    public String toString() {
        return expr + " as " + alias;
    }
}