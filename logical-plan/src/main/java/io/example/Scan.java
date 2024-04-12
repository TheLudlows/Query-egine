package io.example;

import org.example.DataSource;
import org.example.Schema;

import java.util.Collections;
import java.util.List;

public class Scan implements LogicalPlan {


    private DataSource dataSource;
    private List<String> projection;
    private Schema schema;


    public DataSource getDataSource() {
        return dataSource;
    }

    public List<String> getProjection() {
        return projection;
    }

    public Schema getSchema() {
        return schema;
    }

    public Scan(DataSource dataSource, List<String> projection) {
        this.dataSource = dataSource;
        this.projection = projection;
        this.schema = deriveSchema();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    private Schema deriveSchema() {
        Schema schema = dataSource.schema();
        if (projection.isEmpty()) {
            return schema;
        } else {
            return schema.select(projection);
        }
    }

    @Override
    public List<LogicalPlan> children() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return projection.isEmpty()
            ? "Scan: " + dataSource.schema() + "; projection=None"
            : "Scan: " + dataSource.schema()  + "; projection=" + projection;
    }
}