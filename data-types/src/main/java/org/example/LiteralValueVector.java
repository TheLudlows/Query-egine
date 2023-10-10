package org.example;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class LiteralValueVector implements ColumnVector{

    ArrowType type;
    Object value;

    int size;

    public LiteralValueVector(ArrowType type, Object value, int size) {
        this.type = type;
        this.value = value;
        this.size = size;
    }

    @Override
    public ArrowType getType() {
        return type;
    }

    @Override
    public Object getValue(int i) {
        if(i < 0 || i >= size) {
            return null;
        }
        return value;
    }

    @Override
    public int size() {
        return size;
    }
}
