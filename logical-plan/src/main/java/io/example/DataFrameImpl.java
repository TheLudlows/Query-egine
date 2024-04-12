package io.example;

import io.example.expr.AggregateExpr;

import org.example.Schema;

import java.util.List;

public class DataFrameImpl implements DataFrame {

    private final LogicalPlan plan;

    public DataFrameImpl(LogicalPlan plan) {
        this.plan = plan;
    }

    @Override
    public DataFrame project(List<LogicalExpr> expr) {
        return new DataFrameImpl(new Projection(plan, expr));
    }

    @Override
    public DataFrame filter(LogicalExpr expr) {
        return new DataFrameImpl(new Selection(plan, expr));
    }

    @Override
    public DataFrame aggregate(
            List<LogicalExpr> groupBy, List<AggregateExpr> aggregateExpr) {
        return new DataFrameImpl(new Aggregate(plan, groupBy, aggregateExpr));
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }

    @Override
    public LogicalPlan logicalPlan() {
        return plan;
    }
}