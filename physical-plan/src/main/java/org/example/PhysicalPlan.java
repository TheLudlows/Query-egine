package org.example;

import java.util.List;
import java.util.stream.IntStream;

/** A physical plan represents an executable piece of code that will produce data. */
public interface PhysicalPlan {
    /**
     * Format a logical plan in human-readable form
     */
    static String format(PhysicalPlan plan, int indent) {
        StringBuilder b = new StringBuilder();
        IntStream.range(0, indent).forEach(i -> b.append("\t"));

        b.append(plan.toString()).append("\n");
        plan.children().forEach(child -> b.append(format(child, indent + 1)));

        return b.toString();
    }

    Schema schema();

    /**
     * Execute a physical plan and produce a series of record batches.
     */
    Iterable<RecordBatch> execute();

    /**
     * Returns the children (inputs) of this physical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */
    List<PhysicalPlan> children();

    default String pretty() {
        return format(this, 0);
    }
}

