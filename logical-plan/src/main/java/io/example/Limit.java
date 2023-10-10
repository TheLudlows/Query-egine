package io.example;

import org.example.Schema;

import java.util.Collections;
import java.util.List;

public class Limit implements LogicalPlan {
    private LogicalPlan input;
    private int limit;

    public Limit(LogicalPlan input, int limit) {
        this.input = input;
        this.limit = limit;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<LogicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public String toString() {
        return "Limit: " + limit;
    }
}