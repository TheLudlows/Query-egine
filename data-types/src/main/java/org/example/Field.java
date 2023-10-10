package org.example;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Collections;

public class Field {
    String name;
    ArrowType dataType;

    public Field(String name, ArrowType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public org.apache.arrow.vector.types.pojo.Field toArrow() {
        FieldType type = new FieldType(true, dataType, null);
        return new org.apache.arrow.vector.types.pojo.Field(name, type, Collections.emptyList());
    }

    public static Field fromArrow(org.apache.arrow.vector.types.pojo.Field field) {
        return new Field(field.getName(), field.getType());
    }

    public String name() {
        return name;
    }

    public Field setName(String name) {
        this.name = name;
        return this;
    }

    public ArrowType getDataType() {
        return dataType;
    }

    public Field setDataType(ArrowType dataType) {
        this.dataType = dataType;
        return this;
    }

    @Override
    public String toString() {
        return "Field{" + "name=" + name  + ", dataType=" + dataType + '}';
    }
}
