package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.Field;

import java.sql.SQLException;

public class Column implements LogicalExpr {
    private String name;

    public String getName() {
        return name;
    }

    public Column(String name) {
        this.name = name;
    }

    @Override
    public Field toField(LogicalPlan input) {
        for (Field field : input.schema().fields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        throw new RuntimeException("No column named '" + name + "' in " + input.schema().fields());
    }

    @Override
    public String toString() {
        return "#" + name;
    }
}