package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.Field;

import java.util.List;

public class ScalarFunction implements LogicalExpr {
    private String name;
    private List<LogicalExpr> args;
    private ArrowType returnType;

    public ScalarFunction(String name, List<LogicalExpr> args, ArrowType returnType) {
        this.name = name;
        this.args = args;
        this.returnType = returnType;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, returnType);
    }

    @Override
    public String toString() {
        return name + "(" + args + ")";
    }
}