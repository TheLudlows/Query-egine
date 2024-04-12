package org.example;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CsvDataSource implements DataSource {
    static Logger logger = Logger.getLogger(CsvDataSource.class.getSimpleName());

    String sourceName;
    String filename;

    Schema schema;

    boolean hasHeader;

    int batchSize;

    @Override
    public String sourceName() {
        return sourceName;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    public CsvDataSource(String sourceName, int batchSize, boolean hasHeader, String filename, Schema schema) {
        this.sourceName = sourceName;
        this.filename = filename;
        this.hasHeader = hasHeader;
        this.batchSize = batchSize;
        if(schema == null) {
            initSchema();
        }
    }

    public CsvDataSource(String filename, String sourceName) {
        this.sourceName = sourceName;
        this.filename = filename;
        this.hasHeader = true;
        this.batchSize = 10;
        initSchema();

    }

    private void initSchema(){
        File file =  new File(filename);
        if(!file.exists())
            throw new IllegalArgumentException(file.getAbsolutePath() + " not found");

        CsvParser parser = new CsvParser(defaultSettings());
        parser.beginParsing(file);
        parser.parseNext();
        String[] headers = parser.getContext().parsedHeaders();
        List<Field> fields = Stream.of(headers).map(e -> new Field(e, ArrowTypes.StringType)).collect(Collectors.toList());
        parser.stopParsing();
        schema = new Schema(fields);
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        logger.info(String.format("scan project %s", projection.toString()));
        File file = new File(filename);
        if (!file.exists()) {
            throw new IllegalArgumentException(filename + " not found");
        }

        Schema readSchema = schema.select(projection);
        CsvParserSettings settings = defaultSettings();
        if (!projection.isEmpty()) {
            settings.selectFields(projection.toArray(new String[0]));
        }
        if (!hasHeader) {
            String[] headers = new String[readSchema.fields.size()];
            for (int i = 0; i < headers.length; i++) {
                headers[i] = schema.fields.get(i).name;
            }
            settings.setHeaders(headers);
        }
        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(file);
        return () -> new CsvReader(batchSize, parser, readSchema);
    }

    private CsvParserSettings defaultSettings() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setDelimiterDetectionEnabled(true);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setSkipEmptyLines(true);
        settings.setAutoClosingEnabled(true);
        return settings;
    }

    static class CsvReader implements Iterator<RecordBatch> {
        Schema schema;

        CsvParser parser;

        int batchSize;

        private RecordBatch next;

        private boolean start;

        public CsvReader(int batchSize, CsvParser parser, Schema schema) {
            this.batchSize = batchSize;
            this.parser = parser;
            this.schema = schema;
        }

        @Override
        public boolean hasNext() {
            if (!start) {
                start = true;
            }
            next = nextBatch();
            return next != null;
        }

        private RecordBatch nextBatch() {
            List<Record> rows = new ArrayList<>(batchSize);
            Record record;
            do {
                record = parser.parseNextRecord();
                if (record != null) {
                    rows.add(record);
                }
            } while (record != null && rows.size() < batchSize);
            if (rows.isEmpty()) {
                return null;
            }

            return toBatch(rows);
        }

        private RecordBatch toBatch(List<Record> rows) {
            VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema.toArrow(), new RootAllocator());
            vectorSchemaRoot.getFieldVectors().forEach(e -> e.setInitialCapacity(rows.size()));
            vectorSchemaRoot.allocateNew();
            for (int i = 0; i < vectorSchemaRoot.getFieldVectors().size(); i++) {
                FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);
                if (fieldVector instanceof VarCharVector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        String val = rows.get(j).getValue(fieldVector.getName(), "");
                        ((VarCharVector) fieldVector).set(j, val.getBytes());
                    });
                } else if (fieldVector instanceof TinyIntVector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        byte val = rows.get(j).getValue(fieldVector.getName(), (byte) 0);
                        ((TinyIntVector) fieldVector).set(j, val);
                    });
                } else if (fieldVector instanceof SmallIntVector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        short val = rows.get(j).getValue(fieldVector.getName(), (short) 0);
                        ((SmallIntVector) fieldVector).set(j, val);
                    });
                } else if (fieldVector instanceof IntVector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        int val = rows.get(j).getValue(fieldVector.getName(), 0);
                        ((IntVector) fieldVector).set(j, val);
                    });
                } else if (fieldVector instanceof BigIntVector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        long val = rows.get(j).getValue(fieldVector.getName(), 0);
                        ((BigIntVector) fieldVector).set(j, val);
                    });
                } else if (fieldVector instanceof Float4Vector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        float val = rows.get(j).getValue(fieldVector.getName(), 0);
                        ((Float4Vector) fieldVector).set(j, val);
                    });
                } else if (fieldVector instanceof Float8Vector) {
                    IntStream.range(0, rows.size()).forEach(j -> {
                        float val = rows.get(j).getValue(fieldVector.getName(), 0);
                        ((Float8Vector) fieldVector).set(j, val);
                    });
                } else {
                    throw new IllegalStateException("No support for reading CSV columns with data type vector");
                }
                fieldVector.setValueCount(rows.size());
            }
            return new RecordBatch(schema,
                vectorSchemaRoot.getFieldVectors().stream().map(ArrowFieldVector::new).collect(Collectors.toList()));

        }

        @Override
        public RecordBatch next() {
            if (!start) {
                hasNext();
            }
            if (next == null) {
                throw new IllegalStateException("Cannot read past the end of " + CsvReader.class.getSimpleName());
            }
            return next;
        }
    }
}
