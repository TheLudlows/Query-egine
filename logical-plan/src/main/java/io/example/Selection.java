package io.example;

import org.example.Schema;

import java.util.Collections;
import java.util.List;

public class Selection implements LogicalPlan {
    private LogicalPlan input;
    private LogicalExpr expr;

    public LogicalPlan getInput() {
        return input;
    }

    public LogicalExpr getExpr() {
        return expr;
    }

    public Selection(LogicalPlan input, LogicalExpr expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<LogicalPlan> children() {
        // selection does not change the schema of the input
        return Collections.singletonList(input);
    }

    @Override
    public String toString() {
        return "Selection: " + expr;
    }
}