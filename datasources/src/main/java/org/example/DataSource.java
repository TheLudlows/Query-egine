package org.example;

import java.util.Iterator;
import java.util.List;


public interface DataSource {

    String sourceName();
    Schema schema();

    Iterable<RecordBatch> scan(List<String> projection);
}