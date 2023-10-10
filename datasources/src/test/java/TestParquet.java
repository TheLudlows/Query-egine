import org.example.ParquetDataSource;
import org.example.RecordBatch;
import org.example.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestParquet {

    @Test
    public void test_read_schema() {
        String path  ="../testdata/alltypes_plain.parquet";
        ParquetDataSource parquetDataSource = new ParquetDataSource(1, "file:/" + new File(path).getAbsolutePath());
        Schema schema = parquetDataSource.schema();
        System.out.println(schema);
        Assert.assertEquals("Schema{fields=[Field{name=id, dataType=Int(32, true)}, Field{name=bool_col, dataType=Bool}, Field{name=tinyint_col, dataType=Int(32, true)}, Field{name=smallint_col, dataType=Int(32, true)}, Field{name=int_col, dataType=Int(32, true)}, Field{name=bigint_col, dataType=Int(64, true)}, Field{name=float_col, dataType=FloatingPoint(SINGLE)}, Field{name=double_col, dataType=FloatingPoint(DOUBLE)}, Field{name=date_string_col, dataType=Binary}, Field{name=string_col, dataType=Binary}, Field{name=timestamp_col, dataType=Timestamp(NANOSECOND, null)}]}"
            ,schema.toString());
    }

    @Test
    public void test_read_data() {
        String path  ="../testdata/alltypes_plain.parquet";
        ParquetDataSource parquetDataSource = new ParquetDataSource(1, "file:/" + new File(path).getAbsolutePath());
        Iterator<RecordBatch> batch = parquetDataSource.scan(Stream.of("id").collect(Collectors.toList())).iterator();

        while (batch.hasNext()) {
            System.out.print(batch.next());
        }

    }
}
