package org.example.expressions;

import org.example.ColumnVector;
import org.example.RecordBatch;

public class ColumnExpression implements Expression {
    private final int i;

    public ColumnExpression(int i) {
        this.i = i;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return input.field(i);
    }

    @Override
    public String toString() {
        return "#" + i;
    }
}