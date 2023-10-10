package io.example;

import com.google.common.collect.Lists;

import io.example.expr.AggregateExpr;

import org.example.Field;
import org.example.Schema;

import java.util.ArrayList;
import java.util.List;

/** Logical plan representing an aggregate query against an input. */
public class Aggregate implements LogicalPlan {
    private LogicalPlan input;
    private List<LogicalExpr> groupExpr;
    private List<AggregateExpr> aggregateExpr;

    public LogicalPlan getInput() {
        return input;
    }

    public List<LogicalExpr> getGroupExpr() {
        return groupExpr;
    }

    public List<AggregateExpr> getAggregateExpr() {
        return aggregateExpr;
    }

    public Aggregate(LogicalPlan input, List<LogicalExpr> groupExpr, List<AggregateExpr> aggregateExpr) {
        this.input = input;
        this.groupExpr = groupExpr;
        this.aggregateExpr = aggregateExpr;
    }

    @Override
    public Schema schema() {
        List<Field> fields = new ArrayList<>();
        for (LogicalExpr expr : groupExpr) {
            fields.add(expr.toField(input));
        }
        for (AggregateExpr expr : aggregateExpr) {
            fields.add(expr.toField(input));
        }
        return new Schema(fields);
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList(input);
    }

    @Override
    public String toString() {
        return String.format("Aggregate: groupExpr=%s, aggregateExpr=%s", groupExpr, aggregateExpr);
    }
}