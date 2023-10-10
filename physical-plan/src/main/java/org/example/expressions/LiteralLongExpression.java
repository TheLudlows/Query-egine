package org.example.expressions;

import org.example.ArrowTypes;
import org.example.ColumnVector;
import org.example.LiteralValueVector;
import org.example.RecordBatch;

public class LiteralLongExpression implements Expression {
    private final long value;

    public LiteralLongExpression(long value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.Int64Type, value, input.rowCount());
    }
}
