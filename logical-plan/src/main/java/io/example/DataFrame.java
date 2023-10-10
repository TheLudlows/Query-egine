package io.example;

import io.example.expr.AggregateExpr;

import org.example.Schema;

public interface DataFrame {

    /**
     * Apply a projection
     */
    DataFrame project(java.util.List<LogicalExpr> expr);

    /**
     * Apply a filter
     */
    DataFrame filter(LogicalExpr expr);

    /**
     * Aggregate
     */
    DataFrame aggregate(java.util.List<LogicalExpr> groupBy, java.util.List<AggregateExpr> aggregateExpr);

    /**
     * Returns the schema of the data that will be produced by this DataFrame.
     */
    Schema schema();

    /**
     * Get the logical plan
     */
    LogicalPlan logicalPlan();
}
