package org.example.expressions;

import java.util.Objects;

public class Common {

    public static String toString(Object v) {
        if (v instanceof byte[]) {
            return new String((byte[]) v);
        } else {
            return Objects.toString(v);
        }
    }

    public static boolean toBool(Object v) {
        if (v instanceof Boolean) {
            return (Boolean) v;
        } else if (v instanceof Number) {
            return ((Number) v).intValue() == 1;
        } else {
            throw new IllegalStateException("Cannot convert value to boolean: " + v);
        }
    }
}
