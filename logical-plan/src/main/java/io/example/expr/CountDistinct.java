package io.example.expr;

import io.example.LogicalExpr;
import io.example.LogicalPlan;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.example.Field;

public class CountDistinct extends AggregateExpr {
    public CountDistinct(LogicalExpr input) {
        super("COUNT DISTINCT", input);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field("COUNT_DISTINCT", new ArrowType.Int(32, false));
    }

    @Override
    public String toString() {
        return "COUNT(DISTINCT " + expr + ")";
    }
}