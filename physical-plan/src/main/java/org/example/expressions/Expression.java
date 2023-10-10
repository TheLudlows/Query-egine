package org.example.expressions;

import org.example.ColumnVector;
import org.example.RecordBatch;

/** Physical representation of an expression. */

public interface Expression {
    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    ColumnVector evaluate(RecordBatch input);
}