import org.example.ArrowTypes;
import org.example.ColumnVector;
import org.example.Field;
import org.example.Fuzzer;
import org.example.RecordBatch;
import org.example.Schema;
import org.example.expressions.CastExpression;
import org.example.expressions.ColumnExpression;
import org.example.expressions.EqExpression;
import org.example.expressions.Expression;
import org.example.expressions.LiteralLongExpression;
import org.junit.Test;

import java.util.Collections;

public class CastTest {

    @Test
    public void testCast() {
        Schema schema = new Schema(Collections.singletonList(new Field("abc", ArrowTypes.Int8Type)));
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, 10);
        System.out.println(batch.toString());
        Expression castExpr = new CastExpression(new ColumnExpression(0), ArrowTypes.StringType);
        ColumnVector val = castExpr.evaluate(batch);
        System.out.println(castExpr);
        System.out.println(val.toString());
    }

    @Test
    public void testBool() {
        Schema schema = new Schema(Collections.singletonList(new Field("abc", ArrowTypes.Int64Type)));
        RecordBatch batch = new Fuzzer().createRecordBatch(schema, 10);
        System.out.println(batch.toString());
        Expression boolExpr = new EqExpression(new ColumnExpression(0), new LiteralLongExpression(0));
        ColumnVector val = boolExpr.evaluate(batch);
        System.out.println(boolExpr);
        System.out.println(val.toString());
    }
}
