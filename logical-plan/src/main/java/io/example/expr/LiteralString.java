package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.example.ArrowTypes;
import org.example.Field;

public class LiteralString implements LogicalExpr {
    private String str;

    public String getStr() {
        return str;
    }

    public LiteralString(String str) {
        this.str = str;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(str, ArrowTypes.StringType);
    }

    @Override
    public String toString() {
        return "'" + str + "'";
    }
}
