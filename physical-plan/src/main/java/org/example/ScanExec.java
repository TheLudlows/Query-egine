package org.example;

import java.util.Collections;
import java.util.List;

public class ScanExec implements PhysicalPlan {

    private final DataSource ds;
    private final List<String> projection;

    public ScanExec(DataSource ds, List<String> projection) {
        this.ds = ds;
        this.projection = projection;
    }

    @Override
    public Schema schema() {
        return ds.schema().select(projection);
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Iterable<RecordBatch> execute() {
        return ds.scan(projection);
    }

    @Override
    public String toString() {
        return "ScanExec: schema=" + schema() + ", projection=" + projection;
    }
}