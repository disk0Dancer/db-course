package com.querifylabs.dbcourse.executor;

import com.querifylabs.dbcourse.executor.aggregation.Accumulator;
import com.querifylabs.dbcourse.executor.aggregation.SumAccumulator;
import org.apache.arrow.vector.*;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

public class AggregationNode extends BaseExecutionNode {
    private final ExecutionNode input;
    private final List<Integer> groupSet;
    private final List<AggregateCall> aggCalls;
    private final RelDataType rowType;

    private boolean processed = false;

    public AggregationNode(ExecutionContext executionContext, ExecutionNode input,
                           List<Integer> groupSet, List<AggregateCall> aggCalls, RelDataType rowType) {
        super(executionContext);
        this.input = input;
        this.groupSet = groupSet;
        this.aggCalls = aggCalls;
        this.rowType = rowType;
    }

    @Override
    public DataPage getNextPage() throws ExecutorException {
        if (processed) return null;
        processed = true;

        Map<List<Object>, List<Accumulator>> map = new HashMap<>();

        while (true) {
            try (DataPage page = input.getNextPage()) {
                if (page == null) break;

                int rowCount = page.getRowCount();
                for (int i = 0; i < rowCount; i++) {
                    List<Object> key = new ArrayList<>();
                    for (int colIdx : groupSet) {
                        key.add(getObject(page.getColumns().get(colIdx), i));
                    }

                    List<Accumulator> accs = map.computeIfAbsent(key, k -> createAccumulators());

                    for (int j = 0; j < aggCalls.size(); j++) {
                        AggregateCall call = aggCalls.get(j);
                        List<Integer> args = call.getArgList();
                        if (args.isEmpty()) {
                            accs.get(j).add(1L);
                        } else {
                            int argIdx = args.get(0);
                            Object val = getObject(page.getColumns().get(argIdx), i);
                            accs.get(j).add(val);
                        }
                    }
                }
            } catch (Exception e) {
                throw new ExecutorException("Aggregation failed", e);
            }
        }

        if (map.isEmpty() && groupSet.isEmpty()) {
            map.put(Collections.emptyList(), createAccumulators());
        }

        if (map.isEmpty()) {
            return null;
        }

        int resultRows = map.size();
        List<FieldVector> vectors = new ArrayList<>();

        for (int i = 0; i < groupSet.size(); i++) {
            FieldVector v = createVector(rowType.getFieldList().get(i).getType().getSqlTypeName(), "g" + i, resultRows);
            vectors.add(v);
        }

        for (int i = 0; i < aggCalls.size(); i++) {
            FieldVector v = createVector(rowType.getFieldList().get(groupSet.size() + i).getType().getSqlTypeName(), "a" + i, resultRows);
            vectors.add(v);
        }

        int rowIdx = 0;
        for (Map.Entry<List<Object>, List<Accumulator>> entry : map.entrySet()) {
            List<Object> key = entry.getKey();
            List<Accumulator> accs = entry.getValue();

            for (int i = 0; i < key.size(); i++) {
                setValue(vectors.get(i), rowIdx, key.get(i));
            }

            for (int i = 0; i < accs.size(); i++) {
                setValue(vectors.get(groupSet.size() + i), rowIdx, accs.get(i).getResult());
            }
            rowIdx++;
        }

        for (FieldVector v : vectors) {
            v.setValueCount(resultRows);
        }

        return new DataPage(vectors);
    }

    private List<Accumulator> createAccumulators() {
        List<Accumulator> list = new ArrayList<>();
        for (AggregateCall call : aggCalls) {
            if (call.getAggregation().getKind() == SqlKind.SUM) {
                list.add(new SumAccumulator());
            } else {
                list.add(new SumAccumulator());
            }
        }
        return list;
    }

    private Object getObject(ValueVector v, int index) {
        if (v.isNull(index)) return null;
        if (v instanceof BigIntVector) return ((BigIntVector) v).get(index);
        if (v instanceof IntVector) return ((IntVector) v).get(index);
        if (v instanceof Float8Vector) return ((Float8Vector) v).get(index);
        if (v instanceof Float4Vector) return ((Float4Vector) v).get(index);
        return v.getObject(index);
    }

    private FieldVector createVector(org.apache.calcite.sql.type.SqlTypeName typeName, String name, int size) {
        FieldVector v;
        switch (typeName) {
            case INTEGER: v = new IntVector(name, executionContext.getAllocator()); break;
            case DOUBLE: v = new Float8Vector(name, executionContext.getAllocator()); break;
            case FLOAT: v = new Float4Vector(name, executionContext.getAllocator()); break;
            default: v = new BigIntVector(name, executionContext.getAllocator());
        }
        v.setInitialCapacity(size);
        v.allocateNew();
        return v;
    }

    private void setValue(FieldVector v, int index, Object val) {
        if (val == null) {
            if (v instanceof BigIntVector) v.setNull(index);
            else if (v instanceof IntVector) v.setNull(index);
            else if (v instanceof Float8Vector) v.setNull(index);
            return;
        }

        if (v instanceof BigIntVector) ((BigIntVector) v).set(index, ((Number) val).longValue());
        else if (v instanceof IntVector) ((IntVector) v).set(index, ((Number) val).intValue());
        else if (v instanceof Float8Vector) ((Float8Vector) v).set(index, ((Number) val).doubleValue());
    }

    @Override
    public void close() throws Exception {
        input.close();
    }
}