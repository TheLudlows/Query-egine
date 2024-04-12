package org.example;

import java.util.List;
import java.util.stream.Collectors;

public class InMemeryDataSource implements DataSource {

    String sourceName;
    Schema schema;

    List<RecordBatch> batches;

    public InMemeryDataSource(String sourceName, Schema schema, List<RecordBatch> batches) {
        this.sourceName = sourceName;
        this.schema = schema;
        this.batches = batches;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        List<Integer> idx = projection.stream().map(name -> schema.indexOf(name)).collect(Collectors.toList());
        return () -> batches.stream()
            .map(e -> new RecordBatch(schema.select(projection),
                idx.stream().map(i -> e.fields.get(i)).collect(Collectors.toList())))
            .iterator();
    }

    @Override
    public String sourceName() {
        return sourceName;
    }

}
