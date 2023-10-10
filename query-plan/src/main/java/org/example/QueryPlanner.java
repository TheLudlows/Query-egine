package org.example;

import io.example.Aggregate;
import io.example.LogicalExpr;
import io.example.LogicalPlan;
import io.example.Projection;
import io.example.Scan;
import io.example.Selection;
import io.example.expr.*;

import org.example.expressions.AggregateExpression;
import org.example.expressions.Expression;
import org.example.expressions.*;
import org.example.expressions.MaxExpression;
import org.example.expressions.MinExpression;
import org.example.expressions.SumExpression;

import java.lang.IllegalStateException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryPlanner {

    public static PhysicalPlan createPhysicalPlan(LogicalPlan plan) {
        if (plan instanceof Scan) {
            Scan scan = (Scan) plan;
            return new ScanExec(scan.getDataSource(), scan.getProjection());
        } else if (plan instanceof Selection) {
            Selection selection = (Selection) plan;
            PhysicalPlan input = createPhysicalPlan(selection.getInput());
            Expression filterExpr = createPhysicalExpr(selection.getExpr(), selection.getInput());
            return new SelectionExec(input, filterExpr);
        } else if (plan instanceof Projection) {
            Projection projection = (Projection) plan;
            PhysicalPlan input = createPhysicalPlan(projection.getInput());
            List<Expression> projectionExpr = new ArrayList<>();
            List<LogicalExpr> exprList = projection.getExpr();
            for (LogicalExpr expr : exprList) {
                projectionExpr.add(createPhysicalExpr(expr, projection.getInput()));
            }
            Schema projectionSchema = new Schema(projection.getExpr().stream()
                .map(expr -> expr.toField(projection.getInput()))
                .collect(Collectors.toList()));
            return new ProjectionExec(input, projectionSchema, projectionExpr);
        } else if (plan instanceof Aggregate) {
            Aggregate aggregate = (Aggregate) plan;
            PhysicalPlan input = createPhysicalPlan(aggregate.getInput());
            List<Expression> groupExpr = new ArrayList<>();
            List<LogicalExpr> groupExprList = aggregate.getGroupExpr();
            for (LogicalExpr expr : groupExprList) {
                groupExpr.add(createPhysicalExpr(expr, aggregate.getInput()));
            }
            List<AggregateExpression> aggregateExpr = new ArrayList<>();
            List<AggregateExpr> aggregateExprList = aggregate.getAggregateExpr();
            for (LogicalExpr expr : aggregateExprList) {
                if (expr instanceof Max) {
                    Max max = (Max) expr;
                    aggregateExpr.add(new MaxExpression(createPhysicalExpr(max.getExpr(), aggregate.getInput())));
                } else if (expr instanceof Min) {
                    Min min = (Min) expr;
                    aggregateExpr.add(new MinExpression(createPhysicalExpr(min.getExpr(), aggregate.getInput())));
                } else if (expr instanceof Sum) {
                    Sum sum = (Sum) expr;
                    aggregateExpr.add(new SumExpression(createPhysicalExpr(sum.getExpr(), aggregate.getInput())));
                } else {
                    throw new IllegalStateException("Unsupported aggregate function: " + expr);
                }
            }
            return new HashAggregateExec(input, groupExpr, aggregateExpr, aggregate.schema());
        } else {
            throw new IllegalStateException(plan.getClass().toString());
        }
    }


    public static Expression createPhysicalExpr(LogicalExpr expr, LogicalPlan input) {
        if (expr instanceof LiteralLong) {
            LiteralLong literalLong = (LiteralLong) expr;
            return new LiteralLongExpression(literalLong.getN());
        } else if (expr instanceof LiteralDouble) {
            LiteralDouble literalDouble = (LiteralDouble) expr;
            return new LiteralDoubleExpression(literalDouble.getN());
        } else if (expr instanceof LiteralString) {
            LiteralString literalString = (LiteralString) expr;
            return new LiteralStringExpression(literalString.getStr());
        } else if (expr instanceof ColumnIndex) {
            ColumnIndex columnIndex = (ColumnIndex) expr;
            return new ColumnExpression(columnIndex.getI());
        } else if (expr instanceof Alias) {
            Alias alias = (Alias) expr;
            return createPhysicalExpr(alias.getExpr(), input);
        } else if (expr instanceof Column) {
            Column column = (Column) expr;
            int i = -1;
            List<Field> fields = input.schema().fields();
            for (int j = 0; j < fields.size(); j++) {
                if (fields.get(j).name().equals(column.getName())) {
                    i = j;
                    break;
                }
            }
            if (i == -1) {
                throw new RuntimeException("No column named '" + column.getName() + "'");
            }
            return new ColumnExpression(i);
        } else if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            Expression subExpression = createPhysicalExpr(castExpr.getExpr(), input);
            return new CastExpression(subExpression, castExpr.getDataType());
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr binaryExpr = (BinaryExpr) expr;
            Expression left = createPhysicalExpr(binaryExpr.getL(), input);
            Expression right = createPhysicalExpr(binaryExpr.getR(), input);
            if (binaryExpr instanceof Eq) {
                return new EqExpression(left, right);
            } else if (binaryExpr instanceof Neq) {
                return new NeqExpression(left, right);
            } else if (binaryExpr instanceof Gt) {
                return new GtExpression(left, right);
            } else if (binaryExpr instanceof GtEq) {
                return new GtEqExpression(left, right);
            } else if (binaryExpr instanceof Lt) {
                return new LtExpression(left, right);
            } else if (binaryExpr instanceof LtEq) {
                return new LtEqExpression(left, right);
            } else if (binaryExpr instanceof And) {
                return new AndExpression(left, right);
            } else if (binaryExpr instanceof Or) {
                return new OrExpression(left, right);
            } else if (binaryExpr instanceof Add) {
                return new AddExpression(left, right);
            } else if (binaryExpr instanceof Subtract) {
                return new SubtractExpression(left, right);
            } else if (binaryExpr instanceof Multiply) {
                return new MultiplyExpression(left, right);
            } else if (binaryExpr instanceof Divide) {
                return new DivideExpression(left, right);
            } else {
                throw new IllegalStateException("Unsupported binary expression: " + binaryExpr);
            }
        } else {
            throw new IllegalStateException("Unsupported logical expression: " + expr);
        }
    }

}