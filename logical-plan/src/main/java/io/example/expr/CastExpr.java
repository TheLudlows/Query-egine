package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.Field;

import java.sql.SQLException;

public class CastExpr implements LogicalExpr {
    private LogicalExpr expr;
    private ArrowType dataType;

    public LogicalExpr getExpr() {
        return expr;
    }

    public ArrowType getDataType() {
        return dataType;
    }

    public CastExpr(LogicalExpr expr, ArrowType dataType) {
        this.expr = expr;
        this.dataType = dataType;
    }

    @Override
    public Field toField(LogicalPlan input){
        return new Field(expr.toField(input).name(), dataType);
    }

    @Override
    public String toString() {
        return "CAST(" + expr + " AS " + dataType + ")";
    }
}