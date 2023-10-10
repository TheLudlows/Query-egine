package org.example;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;

public class ArrowVectorBuilder {
    private final FieldVector fieldVector;

    public ArrowVectorBuilder(FieldVector fieldVector) {
        this.fieldVector = fieldVector;
    }

    public void set(int i, Object value) {
        if (fieldVector instanceof VarCharVector) {
            VarCharVector varCharVector = (VarCharVector) fieldVector;
            if (value == null) {
                varCharVector.setNull(i);
            } else if (value instanceof byte[]) {
                varCharVector.set(i, (byte[]) value);
            } else {
                varCharVector.set(i, value.toString().getBytes());
            }
        } else if (fieldVector instanceof TinyIntVector) {
            TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
            if (value == null) {
                tinyIntVector.setNull(i);
            } else if (value instanceof Number) {
                tinyIntVector.set(i, ((Number) value).byteValue());
            } else if (value instanceof String) {
                tinyIntVector.set(i, ((String) value).getBytes()[0]);
            } else {
                throw new IllegalStateException();
            }
        } else if (fieldVector instanceof SmallIntVector) {
            SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
            if (value == null) {
                smallIntVector.setNull(i);
            } else if (value instanceof Number) {
                smallIntVector.set(i, ((Number) value).shortValue());
            } else if (value instanceof String) {
                smallIntVector.set(i, Short.parseShort((String) value));
            } else {
                throw new IllegalStateException();
            }
        } else if (fieldVector instanceof IntVector) {
            IntVector intVector = (IntVector) fieldVector;
            if (value == null) {
                intVector.setNull(i);
            } else if (value instanceof Number) {
                intVector.set(i, ((Number) value).intValue());
            } else if (value instanceof String) {
                intVector.set(i, Integer.parseInt((String) value));
            } else {
                throw new IllegalStateException();
            }
        } else if (fieldVector instanceof BigIntVector) {
            BigIntVector bigIntVector = (BigIntVector) fieldVector;
            if (value == null) {
                bigIntVector.setNull(i);
            } else if (value instanceof Number) {
                bigIntVector.set(i, ((Number) value).longValue());
            } else if (value instanceof String) {
                bigIntVector.set(i, Long.parseLong((String) value));
            } else {
                throw new IllegalStateException();
            }
        } else if (fieldVector instanceof Float4Vector) {
            Float4Vector float4Vector = (Float4Vector) fieldVector;
            if (value == null) {
                float4Vector.setNull(i);
            } else if (value instanceof Number) {
                float4Vector.set(i, ((Number) value).floatValue());
            } else if (value instanceof String) {
                float4Vector.set(i, Float.parseFloat((String) value));
            } else {
                throw new IllegalStateException();
            }
        } else if (fieldVector instanceof Float8Vector) {
            Float8Vector float8Vector = (Float8Vector) fieldVector;
            if (value == null) {
                float8Vector.setNull(i);
            } else if (value instanceof Number) {
                float8Vector.set(i, ((Number) value).doubleValue());
            } else if (value instanceof String) {
                float8Vector.set(i, Double.parseDouble((String) value));
            } else {
                throw new IllegalStateException();
            }
        } else {
            throw new IllegalStateException(fieldVector.getClass().getName());
        }
    }

    public void setValueCount(int n) {
        fieldVector.setValueCount(n);
    }

    public ColumnVector build() {
        return new ArrowFieldVector(fieldVector);
    }
}