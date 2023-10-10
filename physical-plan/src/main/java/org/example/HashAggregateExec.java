package org.example;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.example.expressions.Accumulator;
import org.example.expressions.AggregateExpression;
import org.example.expressions.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HashAggregateExec implements PhysicalPlan {
    private PhysicalPlan input;
    private List<Expression> groupExpr;
    private List<AggregateExpression> aggregateExpr;
    private Schema schema;

    public HashAggregateExec(PhysicalPlan input, List<Expression> groupExpr,
        List<AggregateExpression> aggregateExpr, Schema schema) {
        this.input = input;
        this.groupExpr = groupExpr;
        this.aggregateExpr = aggregateExpr;
        this.schema = schema;
    }

    public Schema schema() {
        return schema;
    }

    public List<PhysicalPlan> children() {
        return Collections.singletonList(input);
    }

    public String toString() {
        return "HashAggregateExec: groupExpr=" + groupExpr + ", aggrExpr=" + aggregateExpr;
    }

    public Iterable<RecordBatch> execute() {
        Map<List<Object>, List<Accumulator>> map = new HashMap<>();

        Iterator<RecordBatch> iterator = input.execute().iterator();
        while (iterator.hasNext()) {
            RecordBatch batch = iterator.next();
            List<Object> groupKeys = groupExpr.stream().map(e -> e.evaluate(batch)).collect(Collectors.toList());
            List<ColumnVector> aggrInputValues = aggregateExpr.stream().map(e -> e.inputExpression().evaluate(batch)).collect(
                Collectors.toList());

            for (int rowIndex = 0; rowIndex < batch.rowCount(); rowIndex++) {
                List<Object> rowKey = new ArrayList<>();
                for (Object value : groupKeys) {
                    if (value instanceof byte[]) {
                        rowKey.add(new String((byte[]) value));
                    } else {
                        rowKey.add(value);
                    }
                }
                System.out.println(rowKey);
                List<Accumulator> accumulators = map.getOrDefault(rowKey, new ArrayList<>());
                if (accumulators.isEmpty()) {
                    for (AggregateExpression aggrExpr : aggregateExpr) {
                        accumulators.add(aggrExpr.createAccumulator());
                    }
                    map.put(rowKey, accumulators);
                }

                for (int i = 0; i < aggregateExpr.size(); i++) {
                    Object value = aggrInputValues.get(i).getValue(rowIndex);
                    accumulators.get(i).accumulate(value);
                }
            }
        }

        org.apache.arrow.vector.types.pojo.Schema arrowSchema = schema.toArrow();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();
        root.setRowCount(map.size());

        List<ArrowVectorBuilder> builders = new ArrayList<>();
        for (FieldVector fieldVector : root.getFieldVectors()) {
            builders.add(new ArrowVectorBuilder(fieldVector));
        }

        int rowIndex = 0;
        for (Map.Entry<List<Object>, List<Accumulator>> entry : map.entrySet()) {
            List<Object> groupingKey = entry.getKey();
            List<Accumulator> accumulators = entry.getValue();

            for (int i = 0; i < groupExpr.size(); i++) {
                builders.get(i).set(rowIndex, groupingKey.get(i));
            }
            for (int i = 0; i < aggregateExpr.size(); i++) {
                builders.get(groupExpr.size() + i).set(rowIndex, accumulators.get(i).finalValue());
            }

            rowIndex++;
        }

        List<FieldVector> fieldVectors = new ArrayList<>(root.getFieldVectors());
        RecordBatch outputBatch = new RecordBatch(schema, fieldVectors.stream().map(ArrowFieldVector::new).collect(
            Collectors.toList()));

        return Collections.singletonList(outputBatch);
    }
}