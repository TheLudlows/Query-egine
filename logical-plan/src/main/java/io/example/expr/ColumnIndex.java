package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.Field;

public class ColumnIndex implements LogicalExpr {
    private int i;

    public int getI() {
        return i;
    }

    public ColumnIndex(int i) {
        this.i = i;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return input.schema().fields().get(i);
    }

    @Override
    public String toString() {
        return "#" + i;
    }
}