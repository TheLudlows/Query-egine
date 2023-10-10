package org.example;

import com.google.common.collect.Iterables;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.example.expressions.Expression;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SelectionExec implements PhysicalPlan {
    private final PhysicalPlan input;

    private final Expression expr;

    public SelectionExec(PhysicalPlan input, Expression expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.singletonList(input);
    }

    @Override
    public Iterable<RecordBatch> execute() {
        Iterable<RecordBatch> it = this.input.execute();
        return Iterables.transform(it, batch -> {
            FieldVector fieldVector = ((ArrowFieldVector) expr.evaluate(batch)).field;
            BitVector bitVector = (BitVector) fieldVector;
            List<ColumnVector> filtered = batch.fields.stream().map(columnVector -> new ArrowFieldVector(filter(columnVector, bitVector))).collect(Collectors.toList());
            return new RecordBatch(batch.schema, filtered);
        });
    }

    private FieldVector filter(ColumnVector v, BitVector selection) {
        FieldVector filteredVector = FieldVectorFactory.create(v.getType(), 0);
        ArrowVectorBuilder builder = new ArrowVectorBuilder(filteredVector);
        int count = 0;
        for (int i = 0; i < selection.getValueCount(); i++) {
            if (selection.get(i) == 1) {
                builder.set(count, v.getValue(i));
                count++;
            }
        }
        filteredVector.setValueCount(count);
        return filteredVector;
    }

    @Override
    public String toString() {
        return "SelectionExec :" + "input=" + input + ", expr=" + expr;
    }
}
