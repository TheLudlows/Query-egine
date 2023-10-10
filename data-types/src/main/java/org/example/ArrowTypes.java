package org.example;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

public interface ArrowTypes {

    ArrowType BooleanType = new ArrowType.Bool();

    ArrowType Int8Type = new ArrowType.Int(8, true);

    ArrowType Int16Type = new ArrowType.Int(16, true);

    ArrowType Int32Type = new ArrowType.Int(32, true);

    ArrowType Int64Type = new ArrowType.Int(64, true);

    ArrowType UInt8Type = new ArrowType.Int(8, false);

    ArrowType UInt16Type = new ArrowType.Int(16, false);

    ArrowType UInt32Type = new ArrowType.Int(32, false);

    ArrowType UInt64Type = new ArrowType.Int(64, false);

    ArrowType FloatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

    ArrowType DoubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    ArrowType StringType = new ArrowType.Utf8();
}
