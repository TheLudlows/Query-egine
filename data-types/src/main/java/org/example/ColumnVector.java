package org.example;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface ColumnVector {
    ArrowType getType();

    Object getValue(int i);

    int size();
}
