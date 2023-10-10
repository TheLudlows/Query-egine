package io.example;

import org.example.Field;
import org.example.Schema;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Projection implements LogicalPlan {
    private LogicalPlan input;
    private List<LogicalExpr> expr;

    public LogicalPlan getInput() {
        return input;
    }

    public List<LogicalExpr> getExpr() {
        return expr;
    }

    public Projection(LogicalPlan input, List<LogicalExpr> expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        List<Field> fields = expr.stream()
            .map(e -> e.toField(input))
            .collect(Collectors.toList());
        return new Schema(fields);
    }

    @Override
    public List<LogicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public String toString() {
        String exprString = expr.stream()
            .map(Object::toString)
            .collect(Collectors.joining(", "));
        return "Projection: " + exprString;
    }
}