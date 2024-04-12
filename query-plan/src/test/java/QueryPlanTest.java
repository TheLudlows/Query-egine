import static java.util.Arrays.asList;

import io.example.DataFrame;
import io.example.DataFrameImpl;
import io.example.LogicalPlan;
import io.example.Projection;
import io.example.Scan;
import io.example.expr.ColumnIndex;
import io.example.expr.GtEq;
import io.example.expr.LiteralLong;

import org.example.ArrowTypes;
import org.example.Field;
import org.example.InMemeryDataSource;
import org.example.PhysicalPlan;
import org.example.QueryPlanner;
import org.example.RecordBatch;
import org.example.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryPlanTest {


    @Test
    public void testQueryPlan() {
        Schema schema = new Schema(asList(new Field("id", ArrowTypes.Int64Type),
            new Field("name", ArrowTypes.StringType)));
        List<RecordBatch> batchList = Collections.emptyList();

        DataFrame frame = new DataFrameImpl(new Scan( new InMemeryDataSource("", schema, batchList), Collections.emptyList()));

        LogicalPlan plan = frame.project(asList(new ColumnIndex(0))).filter(new GtEq(new ColumnIndex(0), new LiteralLong(10)))
            .logicalPlan();
        System.out.println(plan.pretty());
        PhysicalPlan physicalPlan =  QueryPlanner.createPhysicalPlan(plan);
        System.out.println(physicalPlan.pretty());
    }

}
