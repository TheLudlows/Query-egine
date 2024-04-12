package io.example;

import org.example.CsvDataSource;
import org.example.ParquetDataSource;

public class ExecutionContext {

    public DataFrame csv(String filename) {
        return new DataFrameImpl(new Scan(new CsvDataSource(filename, ""), new java.util.ArrayList<>()));
    }

    public DataFrame parquet(String filename) {
        return new DataFrameImpl(new Scan(new ParquetDataSource(filename, ""), new java.util.ArrayList<>()));
    }
}