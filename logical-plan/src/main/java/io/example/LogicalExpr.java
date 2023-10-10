package io.example;

import io.example.expr.Add;
import io.example.expr.Alias;
import io.example.expr.Divide;
import io.example.expr.Eq;
import io.example.expr.Gt;
import io.example.expr.GtEq;
import io.example.expr.Lt;
import io.example.expr.LtEq;
import io.example.expr.Modulus;
import io.example.expr.Multiply;
import io.example.expr.Neq;
import io.example.expr.Subtract;

import org.example.Field;

public interface LogicalExpr {

    Field toField(LogicalPlan input);


    default LogicalExpr eq(LogicalExpr rhs) {
        return new Eq(this, rhs);
    }

    /** Convenience method to create an inequality expression using an infix operator */
    default LogicalExpr neq(LogicalExpr rhs) {
        return new Neq(this, rhs);
    }

    /** Convenience method to create a greater than expression using an infix operator */
    default LogicalExpr gt(LogicalExpr rhs) {
        return new Gt(this, rhs);
    }

    /** Convenience method to create a greater than or equals expression using an infix operator */
    default LogicalExpr gteq(LogicalExpr rhs) {
        return new GtEq(this, rhs);
    }

    /** Convenience method to create a less than expression using an infix operator */
    default LogicalExpr lt(LogicalExpr rhs) {
        return new Lt(this, rhs);
    }

    /** Convenience method to create a less than or equals expression using an infix operator */
    default LogicalExpr lteq(LogicalExpr rhs) {
        return new LtEq(this, rhs);
    }

    /** Convenience method to create a multiplication expression using an infix operator */
    default LogicalExpr add(LogicalExpr rhs) {
        return new Add(this, rhs);
    }

    /** Convenience method to create a multiplication expression using an infix operator */
    default LogicalExpr subtract(LogicalExpr rhs) {
        return new Subtract(this, rhs);
    }

    /** Convenience method to create a multiplication expression using an infix operator */
    default LogicalExpr mult(LogicalExpr rhs) {
        return new Multiply(this, rhs);
    }

    /** Convenience method to create a multiplication expression using an infix operator */
    default LogicalExpr div(LogicalExpr rhs) {
        return new Divide(this, rhs);
    }

    /** Convenience method to create a multiplication expression using an infix operator */
    default LogicalExpr mod(LogicalExpr rhs) {
        return new Modulus(this, rhs);
    }

    default Alias alias(String alias) {
        return new Alias(this, alias);
    }
}
