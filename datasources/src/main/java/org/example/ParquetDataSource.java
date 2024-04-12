package org.example;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ParquetDataSource implements DataSource {

    String sourceName;
    Schema schema;

    int batchSize;

    String fileName;

    public ParquetDataSource(int batchSize, String fileName) {
        this.batchSize = batchSize;
        this.fileName = fileName;
        initSchema();
    }

    public ParquetDataSource(String fileName, String sourceName) {
        this.sourceName = sourceName;
        this.batchSize = 10;
        this.fileName = fileName;
        initSchema();
    }

    @Override
    public String sourceName() {
        return sourceName;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    private void initSchema() {
        ScanOptions options = new ScanOptions(10);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, fileName);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        schema = Schema.formArrow(scanner.schema());
        try {
            scanner.close();
            datasetFactory.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        return new ParquetScan(fileName, projection, schema, batchSize);
    }

    static class ParquetScan implements AutoCloseable, Iterable {
        String fileName;

        ArrowReader reader;

        int batchSize;

        List<String> projection;

        Schema schema;

        public ParquetScan(String fileName, List<String> project, Schema schema, int batchSize) {
            this.fileName = fileName;
            projection = project;
            this.schema = schema;
            this.batchSize = batchSize;
            initScanner();

        }

        @Override
        public void close() throws Exception {
            reader.close();
        }

        @Override
        public Iterator<RecordBatch> iterator() {
            return new ParquetIterator(reader, schema.select(projection));
        }

        public void initScanner() {

            ScanOptions options = new ScanOptions(batchSize, Optional.of(projection.toArray(new String[0])));
            BufferAllocator allocator = new RootAllocator();
            DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, fileName);
            Dataset dataset = datasetFactory.finish();
            Scanner scanner = dataset.newScan(options);
            reader = scanner.scanBatches();
        }
    }

    static class ParquetIterator implements Iterator<RecordBatch> {
        ArrowReader reader;

        Schema schema;

        public ParquetIterator(ArrowReader reader, Schema schema) {
            this.schema = schema;
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            try {
                return reader.loadNextBatch();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public RecordBatch next() {
            try {
                List<ColumnVector> columnVectors = reader.getVectorSchemaRoot()
                    .getFieldVectors()
                    .stream()
                    .map(ArrowFieldVector::new)
                    .collect(Collectors.toList());
                return new RecordBatch(schema, columnVectors);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
