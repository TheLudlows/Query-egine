package org.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class InMemeryDataSource implements DataSource {

    Schema schema;

    List<RecordBatch> batches;

    public InMemeryDataSource(Schema schema, List<RecordBatch> batches) {
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

}
