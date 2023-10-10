package org.example.expressions;

import static org.example.ArrowTypes.*;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.ArrowVectorBuilder;
import org.example.ColumnVector;
import org.example.FieldVectorFactory;
import org.example.RecordBatch;

public class CastExpression implements Expression {
    private final Expression expr;

    private final ArrowType dataType;

    public CastExpression(Expression expr, ArrowType dataType) {
        this.expr = expr;
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return "CAST(" + expr + " AS " + dataType + ")";
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        ColumnVector value = expr.evaluate(input);
        FieldVector fieldVector = FieldVectorFactory.create(dataType, input.rowCount());
        ArrowVectorBuilder builder = new ArrowVectorBuilder(fieldVector);
        if (dataType.equals(Int8Type)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    byte castVal;
                    if (vv instanceof byte[]) {
                        castVal = Byte.parseByte(String.valueOf(vv));
                    } else if (vv instanceof String) {
                        castVal = Byte.parseByte(vv.toString());
                    } else if (vv instanceof Number) {
                        castVal = ((Number) vv).byteValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Byte: $vv");
                    }
                    builder.set(i, castVal);
                }
            }
        } else if (dataType.equals(Int16Type)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    short castVal;
                    if (vv instanceof byte[]) {
                        castVal = Short.parseShort(String.valueOf(vv));
                    } else if (vv instanceof String) {
                        castVal = Short.parseShort(vv.toString());
                    } else if (vv instanceof Number) {
                        castVal = ((Number) vv).shortValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Short: $vv");
                    }
                    builder.set(i, castVal);
                }
            }
        } else if (dataType.equals(Int32Type)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    int castVal;
                    if (vv instanceof byte[]) {
                        castVal = Integer.parseInt(String.valueOf(vv));
                    } else if (vv instanceof String) {
                        castVal = Integer.parseInt(vv.toString());
                    } else if (vv instanceof Number) {
                        castVal = ((Number) vv).intValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Int: $vv");
                    }
                    builder.set(i, castVal);
                }
            }
        } else if (dataType.equals(Int64Type)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    long castVal;
                    if (vv instanceof byte[]) {
                        castVal = Long.parseLong(String.valueOf(vv));
                    } else if (vv instanceof String) {
                        castVal = Long.parseLong(vv.toString());
                    } else if (vv instanceof Number) {
                        castVal = ((Number) vv).intValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Long: $vv");
                    }
                    builder.set(i, castVal);
                }
            }
        } else if (dataType.equals(FloatType)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    float castVal;
                    if (vv instanceof byte[]) {
                        castVal = Float.parseFloat(String.valueOf(vv));
                    } else if (vv instanceof String) {
                        castVal = Float.parseFloat(vv.toString());
                    } else if (vv instanceof Number) {
                        castVal = ((Number) vv).floatValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to Float: $vv");
                    }
                    builder.set(i, castVal);
                }
            }
        } else if (dataType.equals(DoubleType)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    double castVal;
                    if (vv instanceof byte[]) {
                        castVal = Double.parseDouble(String.valueOf(vv));
                    } else if (vv instanceof String) {
                        castVal = Double.parseDouble(vv.toString());
                    } else if (vv instanceof Number) {
                        castVal = ((Number) vv).doubleValue();
                    } else {
                        throw new IllegalStateException("Cannot cast value to double: $vv");
                    }
                    builder.set(i, castVal);
                }
            }
        } else if (dataType.equals(StringType)) {
            for (int i = 0; i < value.size(); i++) {
                Object vv = value.getValue(i);
                if (vv == null) {
                    builder.set(i, null);
                } else {
                    builder.set(i, vv.toString());
                }
            }
        }

        builder.setValueCount(value.size());
        return builder.build();
    }
}