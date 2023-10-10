package org.example.expressions;

import org.example.ArrowTypes;
import org.example.ColumnVector;
import org.example.LiteralValueVector;
import org.example.RecordBatch;

public class LiteralDoubleExpression implements Expression {
    private final double value;

    public LiteralDoubleExpression(double value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.DoubleType, value, input.rowCount());
    }
}
