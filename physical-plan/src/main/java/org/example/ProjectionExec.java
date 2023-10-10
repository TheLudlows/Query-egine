package org.example;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.commons.collections.IterableMap;
import org.example.expressions.Expression;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ProjectionExec implements PhysicalPlan {

    private final PhysicalPlan input;

    private Schema schema;

    private List<Expression> expr;

    public ProjectionExec(PhysicalPlan input, Schema schema, List<Expression> expr) {
        this.input = input;
        this.schema = schema;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public Iterable<RecordBatch> execute() {
        return Iterables.transform(input.execute(), batch -> {
            List<ColumnVector> columnVectors = expr.stream().map(e -> e.evaluate(batch)).collect(Collectors.toList());
            return new RecordBatch(schema, columnVectors);
        });

    }

    @Override
    public String toString() {
        return "ProjectionExec: " + expr;
    }
}