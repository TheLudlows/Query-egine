package io.example.expr;

import io.example.LogicalExpr;

public abstract class BinaryExpr implements LogicalExpr {
    protected String name;
    protected String op;
    protected LogicalExpr l;
    protected LogicalExpr r;

    public String getName() {
        return name;
    }

    public String getOp() {
        return op;
    }

    public LogicalExpr getL() {
        return l;
    }

    public LogicalExpr getR() {
        return r;
    }

    public BinaryExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        this.name = name;
        this.op = op;
        this.l = l;
        this.r = r;
    }

    @Override
    public String toString() {
        return l + " " + op + " " + r;
    }
}