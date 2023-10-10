package io.example;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.util.Lists;
import org.example.Field;
import org.example.Schema;

import java.util.List;
import java.util.stream.Collectors;

public class Join implements LogicalPlan {
    private LogicalPlan left;
    private LogicalPlan right;
    private JoinType joinType;
    private List<Pair<String, String>> on;

    public Join(LogicalPlan left, LogicalPlan right, JoinType joinType, List<Pair<String, String>> on) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.on = on;
    }

    @Override
    public Schema schema() {
        List<String> duplicateKeys = on.stream().filter(pair -> pair.getLeft().equals(pair.getRight()))
            .map(Pair::getLeft).collect(Collectors.toList());
        List<Field> fields;
        switch (joinType) {
            case Inner:
            case Left:
                List<Field> leftFields = left.schema().fields();
                List<Field> rightFields = right.schema().fields().stream().filter(field -> !duplicateKeys.contains(field.name()))
                    .collect(Collectors.toList());
                fields = ListUtils.union(leftFields, rightFields);
                break;
            case Right:
                leftFields = left.schema().fields().stream().filter(field -> !duplicateKeys.contains(field.name()))
                    .collect(Collectors.toList());
                rightFields = right.schema().fields();
                fields = ListUtils.union(leftFields, rightFields);
                break;
            default:
                throw new RuntimeException("Invalid join type: " + joinType);
        }
        return new Schema(fields);
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList(this.left, this.right);
    }
}