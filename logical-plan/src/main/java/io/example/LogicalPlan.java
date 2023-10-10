package io.example;

import org.example.Schema;

import java.util.List;

/**
 * A logical plan represents a data transformation or action that returns a relation (a set of
 * tuples).
 */
public interface LogicalPlan {
    /**
     * Returns the schema of the data that will be produced by this logical plan.
     * It returns a Schema object.
     */
    Schema schema();

    /**
     * Returns the children (inputs) of this logical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     * It returns a List of LogicalPlan objects.
     */
    List<LogicalPlan> children();

    default String pretty() {
        return format(this);
    }

    /** Format a logical plan in human-readable form */
    static String format(LogicalPlan plan, int indent) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            builder.append("\t");
        }
        builder.append(plan.toString()).append("\n");
        List<LogicalPlan> children = plan.children();
        for (LogicalPlan child : children) {
            builder.append(format(child, indent + 1));
        }
        return builder.toString();
    }

    static String format(LogicalPlan plan) {
        return format(plan, 0);
    }
}

