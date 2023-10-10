package org.example.expressions;

import org.example.ArrowTypes;
import org.example.ColumnVector;
import org.example.LiteralValueVector;
import org.example.RecordBatch;

public class LiteralStringExpression implements Expression {
    private final String value;

    public LiteralStringExpression(String value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.StringType, value.getBytes(), input.rowCount());
    }
}

