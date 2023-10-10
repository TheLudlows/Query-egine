package org.example;

import static org.example.ArrowTypes.*;

import io.example.DataFrame;
import io.example.LogicalExpr;
import io.example.expr.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.*;
 class EnhancedRandom {

    private Random rand;

    private List<Character> charPool = new ArrayList<>();

    public EnhancedRandom(Random rand) {
        this.rand = rand;
        this.charPool.addAll(getCharRange('a', 'z'));
        this.charPool.addAll(getCharRange('A', 'Z'));
        this.charPool.addAll(getCharRange('0', '9'));
    }

    private List<Character> getCharRange(char start, char end) {
        List<Character> range = new ArrayList<>();
        for (char c = start; c <= end; c++) {
            range.add(c);
        }
        return range;
    }

    public byte nextByte() {
        switch (rand.nextInt(5)) {
            case 0:
                return Byte.MIN_VALUE;
            case 1:
                return Byte.MAX_VALUE;
            case 2:
            case 3:
                return 0;
            case 4:
                return (byte) rand.nextInt();
            default:
                throw new IllegalStateException();
        }
    }

    public short nextShort() {
        switch (rand.nextInt(5)) {
            case 0:
                return Short.MIN_VALUE;
            case 1:
                return Short.MAX_VALUE;
            case 2:
                return 0;
            case 3:
                return 0;
            case 4:
                return (short) rand.nextInt();
            default:
                throw new IllegalStateException();
        }
    }

    public int nextInt() {
        switch (rand.nextInt(5)) {
            case 0:
                return Integer.MIN_VALUE;
            case 1:
                return Integer.MAX_VALUE;
            case 2:
                return 0;
            case 3:
                return 0;
            case 4:
                return rand.nextInt();
            default:
                throw new IllegalStateException();
        }
    }

    public long nextLong() {
        switch (rand.nextInt(5)) {
            case 0:
                return Long.MIN_VALUE;
            case 1:
                return Long.MAX_VALUE;
            case 2:
            case 3:
                return 0;
            case 4:
                return rand.nextLong();
            default:
                throw new IllegalStateException();
        }
    }

    public double nextDouble() {
        switch (rand.nextInt(8)) {
            case 0:
                return Double.MIN_VALUE;
            case 1:
                return Double.MAX_VALUE;
            case 2:
                return Double.POSITIVE_INFINITY;
            case 3:
                return Double.NEGATIVE_INFINITY;
            case 4:
                return Double.NaN;
            case 5:
                return -0.0;
            case 6:
                return 0.0;
            case 7:
                return rand.nextDouble();
            default:
                throw new IllegalStateException();
        }
    }

    public float nextFloat() {
        switch (rand.nextInt(8)) {
            case 0:
                return Float.MIN_VALUE;
            case 1:
                return Float.MAX_VALUE;
            case 2:
                return Float.POSITIVE_INFINITY;
            case 3:
                return Float.NEGATIVE_INFINITY;
            case 4:
                return Float.NaN;
            case 5:
                return -0.0f;
            case 6:
                return 0.0f;
            case 7:
                return rand.nextFloat();
            default:
                throw new IllegalStateException();
        }
    }

    public String nextString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append(charPool.get(rand.nextInt(charPool.size())));
        }
        return sb.toString();
    }
}

public class Fuzzer {

    private Random rand = new Random();

    private EnhancedRandom myRand = new EnhancedRandom(rand);

    /**
     * Create a list of random values based on the provided data type
     */
    public List<Object> createValues(ArrowType arrowType, int n) {
        List<Object> values = new ArrayList<>();
        if (arrowType.equals(Int8Type)) {
            for (int i = 0; i < n; i++) {
                values.add((byte) myRand.nextInt());
            }
        } else if (arrowType.equals(Int16Type)) {
            for (int i = 0; i < n; i++) {
                values.add((short) myRand.nextInt());
            }
        } else if (arrowType.equals(Int32Type)) {
            for (int i = 0; i < n; i++) {
                values.add(myRand.nextInt());
            }
        } else if (arrowType.equals(Int64Type)) {
            for (int i = 0; i < n; i++) {
                values.add(myRand.nextLong());
            }
        } else if (arrowType.equals(FloatType)) {
            for (int i = 0; i < n; i++) {
                values.add(myRand.nextFloat());
            }
        } else if (arrowType.equals(DoubleType)) {
            for (int i = 0; i < n; i++) {
                values.add(myRand.nextDouble());
            }
        } else if (arrowType.equals(StringType)) {
            for (int i = 0; i < n; i++) {
                values.add(String.valueOf(myRand.nextString(rand.nextInt(10))));
            }
        } else {
            throw new IllegalStateException();
        }
        return values;
    }

    /**
     * Create a RecordBatch containing random data based on the provided schema
     */
    public RecordBatch createRecordBatch(Schema schema, int n) {
        List<List<Object>> columns = new ArrayList<>();
        for (Field field : schema.fields()) {
            columns.add(createValues(field.getDataType(), n));
        }
        return createRecordBatch(schema, columns);
    }

    /**
     * Create a RecordBatch containing the specified values.
     */
    public RecordBatch createRecordBatch(Schema schema, List<List<Object>> columns) {
        int rowCount = columns.get(0).size();

        VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrow(), new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();

        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < columns.size(); col++) {
                FieldVector v = root.getVector(col);
                Object value = columns.get(col).get(row);
                if (v instanceof BitVector) {
                    ((BitVector) v).set(row, (boolean) value ? 1 : 0);
                } else if (v instanceof TinyIntVector) {
                    ((TinyIntVector) v).set(row, (byte) value);
                } else if (v instanceof SmallIntVector) {
                    ((SmallIntVector) v).set(row, (short) value);
                } else if (v instanceof IntVector) {
                    ((IntVector) v).set(row, (int) value);
                } else if (v instanceof BigIntVector) {
                    ((BigIntVector) v).set(row, (long) value);
                } else if (v instanceof Float4Vector) {
                    ((Float4Vector) v).set(row, (float) value);
                } else if (v instanceof Float8Vector) {
                    ((Float8Vector) v).set(row, (double) value);
                } else if (v instanceof VarCharVector) {
                    ((VarCharVector) v).set(row, ((String) value).getBytes());
                } else {
                    throw new IllegalStateException();
                }
            }
        }
        root.setRowCount(rowCount);

        return new RecordBatch(schema,
            root.getFieldVectors().stream().map(ArrowFieldVector::new).collect(Collectors.toList()));
    }

    public DataFrame createPlan(DataFrame input, int depth, int maxDepth, int maxExprDepth) {
        if (depth == maxDepth) {
            return input;
        } else {
            DataFrame child = createPlan(input, depth + 1, maxDepth, maxExprDepth);
            int ce = rand.nextInt(2);
            if (ce == 0) {
                int exprCount = 1 + rand.nextInt(5);
                List<LogicalExpr> expressions = new ArrayList<>();
                for (int i = 0; i < exprCount; i++) {
                    expressions.add(createExpression(child, 0, maxExprDepth));
                }
                return child.project(expressions);
            } else {
                LogicalExpr expression = createExpression(input, 0, maxExprDepth);
                return child.filter(expression);
            }
        }
    }

    public LogicalExpr createExpression(DataFrame input, int depth, int maxDepth) {
        if (depth == maxDepth) {
            // return a leaf node
            switch (rand.nextInt(4)) {
                case 0:
                    return new ColumnIndex(rand.nextInt(input.schema().fields.size()));
                case 1:
                    return new LiteralDouble(myRand.nextDouble());
                case 2:
                    return new LiteralLong(myRand.nextLong());
                case 3:
                    return new LiteralString(myRand.nextString(rand.nextInt(64)));
                default:
                    throw new IllegalStateException();
            }
        } else {
            // binary expressions
            LogicalExpr l = createExpression(input, depth + 1, maxDepth);
            LogicalExpr r = createExpression(input, depth + 1, maxDepth);
            switch (rand.nextInt(8)) {
                case 0:
                    return new Eq(l, r);
                case 1:
                    return new Neq(l, r);
                case 2:
                    return new Lt(l, r);
                case 3:
                    return new LtEq(l, r);
                case 4:
                    return new Gt(l, r);
                case 5:
                    return new GtEq(l, r);
                case 6:
                    return new And(l, r);
                case 7:
                    return new Or(l, r);
                default:
                    throw new IllegalStateException();
            }
        }
    }

}