package org.example;

import java.util.Iterator;
import java.util.List;


public interface DataSource {

    Schema schema();

    Iterable<RecordBatch> scan(List<String> projection);
}