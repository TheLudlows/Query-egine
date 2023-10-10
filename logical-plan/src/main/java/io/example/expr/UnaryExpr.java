package io.example.expr;

import io.example.LogicalExpr;

public abstract class UnaryExpr implements LogicalExpr {
    protected String name;
    protected String op;
    protected LogicalExpr expr;

    public UnaryExpr(String name, String op, LogicalExpr expr) {
        this.name = name;
        this.op = op;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return op + " " + expr;
    }
}