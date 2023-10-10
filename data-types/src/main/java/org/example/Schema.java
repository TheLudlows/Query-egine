package org.example;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Schema {
    public List<Field> fields;

    public Schema(List<Field> fields) {
        this.fields = fields;
    }


    public org.apache.arrow.vector.types.pojo.Schema toArrow() {
       return new org.apache.arrow.vector.types.pojo.Schema(fields.stream().map(Field::toArrow).collect(Collectors.toList()));
    }

    public Schema project(List<Integer> indices) {
        if(indices == null)
            return null;

       return new Schema(indices.stream().map(e -> this.fields.get(e)).collect(Collectors.toList()));
    }


    public int indexOf(String fieldName) {
        for(int i=0;i<fields.size();i++) {
            if(fieldName.equals(fields.get(i).name))
                return i;
        }
        return -1;
    }
    public Schema select(List<String> indices) {
        if(indices == null)
            return null;
        Map<String, Field> fieldMap = this.fields.stream().collect(Collectors.toMap(Field::name, e->e));

        return new Schema(indices.stream().map(fieldMap::get).collect(Collectors.toList()));
    }

    public static Schema formArrow(org.apache.arrow.vector.types.pojo.Schema schema) {
        return new Schema(schema.getFields().stream().map(Field::fromArrow).collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return "Schema{" + "fields=" + fields + '}';
    }

    public List<Field> fields() {
        return fields;
    }
}
