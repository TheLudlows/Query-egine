import static io.example.LogicalPlan.format;

import io.example.ExecutionContext;
import io.example.LogicalPlan;
import io.example.Projection;
import io.example.Scan;
import io.example.Selection;
import io.example.expr.Column;
import io.example.expr.Eq;
import io.example.expr.LiteralString;
import io.example.expr.Lt;

import org.example.CsvDataSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class TestPlan {
    String csv_path = "../testdata/employee.csv";

    @Test
    public void test() {
        LogicalPlan plan = new Projection(
            new Selection(new Scan("employee", new CsvDataSource(csv_path), Collections.emptyList()),
                new Eq(new Column("3"), new LiteralString("CO"))),
            Arrays.asList(new Column("id"), new Column("first_name"), new Column("last_name"), new Column("state"),
                new Column("salary")));

        System.out.println(format(plan));
    }

    @Test
    public void testDataFrame() {
        ExecutionContext exe = new ExecutionContext();
        LogicalPlan plan = exe.csv(csv_path)
            .filter(new Lt(new Column("id"), new LiteralString("aaaa")))
            .project(Arrays.asList(new Column("id"), new Column("name")))
            .logicalPlan();

        System.out.println(plan.pretty());
    }
}