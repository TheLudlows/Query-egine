package org.example;

import java.util.List;

public class RecordBatch {
    Schema schema;

    List<ColumnVector> fields;

    public RecordBatch(Schema schema, List<ColumnVector> fields) {
        this.schema = schema;
        this.fields = fields;
    }

    public ColumnVector field(int i) {
        return fields.get(i);
    }

    public int rowCount() {
        return fields.get(0).size();
    }

    public int columnCount() {
        return fields.size();
    }

    public String toCSV() {
        StringBuilder builder = new StringBuilder();

        int columnCount = columnCount();
        int rowCount = rowCount();
        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < columnCount; j++) {
                ColumnVector columnVector = fields.get(j);
                if (j > 0) {
                    builder.append(',');
                }
                Object val = columnVector.getValue(i);
                if (val == null) {
                    builder.append("null");
                } else {
                    builder.append(val);
                }
            }
            builder.append('\n');
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return toCSV();
    }
}
