package org.example;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ArrowFieldVector implements ColumnVector {
    FieldVector field;

    public ArrowFieldVector(FieldVector field) {
        this.field = field;
    }

    @Override
    public ArrowType getType() {
        if (field instanceof BitVector) {
            return ArrowTypes.BooleanType;
        }
        if (field instanceof TinyIntVector) {
            return ArrowTypes.Int8Type;
        }
        if (field instanceof SmallIntVector) {
            return ArrowTypes.Int16Type;
        }
        if (field instanceof IntVector) {
            return ArrowTypes.Int32Type;
        }
        if (field instanceof BigIntVector) {
            return ArrowTypes.Int64Type;
        }
        if (field instanceof Float4Vector) {
            return ArrowTypes.FloatType;
        }
        if (field instanceof Float8Vector) {
            return ArrowTypes.DoubleType;
        }
        if (field instanceof VarCharVector) {
            return ArrowTypes.StringType;
        }

        throw new IllegalArgumentException();
    }

    @Override
    public Object getValue(int i) {
        if (field == null || field.isNull(i)) {
            return null;
        }
        if (field instanceof BitVector) {
            return ((BitVector) field).get(i) == 1;
        }
        if (field instanceof TinyIntVector) {
            return ((TinyIntVector) field).get(i);
        }
        if (field instanceof SmallIntVector) {
            return ((SmallIntVector) field).get(i);
        }
        if (field instanceof IntVector) {
            return ((IntVector) field).get(i);
        }
        if (field instanceof BigIntVector) {
            return ((BigIntVector) field).get(i);
        }
        if (field instanceof Float4Vector) {
            return ((Float4Vector) field).get(i);
        }
        if (field instanceof Float8Vector) {
            return ((Float8Vector) field).get(i);
        }
        if (field instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) field).get(i);
            if (bytes == null) {
                return null;
            } else {
                return new String(bytes);
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public int size() {
        return field.getValueCount();
    }

    @Override
    public String toString() {
        return "ArrowFieldVector{" + "field=" + field + '}';
    }
}
