package com.querifylabs.dbcourse.executor;

import com.querifylabs.dbcourse.schema.ParquetTable;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ParquetScanNode extends BaseExecutionNode {
    private final ParquetTable table;
    private final List<Integer> projectedColumns;
    private final List<RexNode> conditions;
    private List<Scanner> scanners = new ArrayList<>();
    private Iterator<? extends ScanTask> scanTaskIterator;
    private ArrowReader currentReader;

    public ParquetScanNode(ExecutionContext executionContext, ParquetTable table, List<Integer> projectedColumns, List<RexNode> conditions) {
        super(executionContext);
        this.table = table;
        this.projectedColumns = projectedColumns;
        this.conditions = conditions;
    }

    @Override
    public DataPage getNextPage() throws ExecutorException {
        if (scanTaskIterator == null) {
            initScanner();
        }

        while (true) {
            if (currentReader != null) {
                try {
                    if (currentReader.loadNextBatch()) {
                        VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
                        return new DataPage(root.getFieldVectors());
                    }
                } catch (Exception e) {
                    throw new ExecutorException("Failed to load next batch", e);
                }
            }

            if (currentReader != null) {
                try {
                    currentReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new ExecutorException("Failed to close scan task reader", e);
                }
                currentReader = null;
            }

            if (scanTaskIterator.hasNext()) {
                ScanTask task = scanTaskIterator.next();
                currentReader = task.execute();
            } else {
                return null;
            }
        }
    }

    private void initScanner() {
        List<File> allFiles = table.getFiles();
        if (allFiles == null || allFiles.isEmpty()) {
            scanTaskIterator = new ArrayList<ScanTask>().iterator();
            return;
        }

        
        Schema s;
        try (DatasetFactory dsFactory = new FileSystemDatasetFactory(
            executionContext.getAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, allFiles.get(0).toURI().toString())) {
            s = dsFactory.inspect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<File> filesToScan = new ArrayList<>();
        
        List<String> fieldNames = s.getFields().stream().map(org.apache.arrow.vector.types.pojo.Field::getName).collect(Collectors.toList());

        for (File f : allFiles) {
            if (!shouldDrop(f, conditions, fieldNames)) {
                filesToScan.add(f);
            }
        }

        List<ScanTask> tasks = new ArrayList<>();

        for (File f : filesToScan) {
            DatasetFactory dsFactory = new FileSystemDatasetFactory(
                executionContext.getAllocator(), NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, f.toURI().toString());

            try {
                Dataset dataset = dsFactory.finish();
                ScanOptions.Builder builder = new ScanOptions.Builder(32768);

                if (projectedColumns != null) {
                    String[] cols = new String[projectedColumns.size()];
                    for (int i = 0; i < projectedColumns.size(); i++) {
                        cols[i] = s.getFields().get(projectedColumns.get(i)).getName();
                    }
                    builder.columns(Optional.of(cols));
                } else {
                    List<org.apache.arrow.vector.types.pojo.Field> fields = s.getFields();
                    String[] cols = new String[fields.size()];
                    for (int i = 0; i < fields.size(); i++) {
                        cols[i] = fields.get(i).getName();
                    }
                    builder.columns(Optional.of(cols));
                }

                Scanner scan = dataset.newScan(builder.build());
                scanners.add(scan);
                scan.scan().forEach(tasks::add);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        scanTaskIterator = tasks.iterator();
    }

    private boolean shouldDrop(File f, List<RexNode> conditions, List<String> fieldNames) {
        if (conditions == null || conditions.isEmpty()) return false;

        try (ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(f.getAbsolutePath()))) {
            ParquetMetadata footer = reader.getFooter();
            for (BlockMetaData block : footer.getBlocks()) {
                boolean blockSatisfies = true;
                if (checkBlock(block, conditions, fieldNames)) {
                     return false; 
                }
            }
            return true; 
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean checkBlock(BlockMetaData block, List<RexNode> conditions, List<String> fieldNames) {
        for (RexNode cond : conditions) {
            if (!checkCondition(block, cond, fieldNames)) {
                return false; 
            }
        }
        return true;
    }

    private boolean checkCondition(BlockMetaData block, RexNode cond, List<String> fieldNames) {
        if (cond instanceof RexCall call) {
            if (call.getKind() == SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    if (!checkCondition(block, operand, fieldNames)) return false;
                }
                return true;
            }

            RexNode op1 = call.getOperands().get(0);
            RexNode op2 = call.getOperands().get(1);

            RexInputRef ref = unwrapInputRef(op1);
            RexLiteral lit = (op2 instanceof RexLiteral) ? (RexLiteral) op2 : null;
            SqlKind kind = call.getKind();

            if (ref == null) {
                ref = unwrapInputRef(op2);
                lit = (op1 instanceof RexLiteral) ? (RexLiteral) op1 : null;
                kind = kind.reverse();
            }

            if (ref != null && lit != null) {
                int refIndex = ref.getIndex();
                Comparable literalVal = (Comparable) lit.getValue();

                if (refIndex >= 0 && refIndex < fieldNames.size()) {
                    String colName = fieldNames.get(refIndex);
                    if (!colName.contains("datetime")) return true;
    private boolean checkCondition(BlockMetaData block, RexNode cond, List<String> fieldNames) {
        
        if (cond instanceof RexCall call) {
            if (call.getKind() == SqlKind.AND) {
                
            }

                    Statistics stats = chunk.getStatistics();
                    if (stats == null || stats.isEmpty()) {
                        System.err.println("Stats missing for " + colName);
                        return true;
                    }

                    return checkStats(stats, kind, literalVal);
                } else {
                    
                }
            }
        }
        return true;
    }

    private RexInputRef unwrapInputRef(RexNode node) {
        if (node instanceof RexInputRef) return (RexInputRef) node;
        if (node instanceof RexCall && node.getKind() == SqlKind.CAST) {
            RexNode op = ((RexCall) node).getOperands().get(0);
            if (op instanceof RexInputRef) return (RexInputRef) op;
        }
        return null;
    }

    private ColumnChunkMetaData getColumnChunk(BlockMetaData block, String name) {
        for (ColumnChunkMetaData c : block.getColumns()) {
            if (c.getPath().toDotString().equalsIgnoreCase(name)) return c;
            if (c.getPath().toDotString().endsWith(name)) return c;
        }
        return null;
    }

    private boolean checkStats(Statistics stats, SqlKind kind, Comparable val) {
        Comparable min = null;
        Comparable max = null;

        if (stats.hasNonNullValue()) {
             min = (Comparable) stats.genericGetMin();
             max = (Comparable) stats.genericGetMax();
        }

        Long v = convertToLong(val);
        Long mn = convertToLong((Comparable) min);
        Long mx = convertToLong((Comparable) max);

        
        
        
        

        if (v == null || mn == null || mx == null) {
            
            return true;
        }

        
        if (mn > 1000000000000000L && v < 10000000000000L) {
             v *= 1000;
             
        }

             if (kind == SqlKind.LESS_THAN) {
                 if (mn >= v) {
                     
                     return false;
                 }
             }
             if (kind == SqlKind.LESS_THAN_OR_EQUAL) {
                 if (mn > v) {
                     
                     return false;
                 }
             }
             if (kind == SqlKind.GREATER_THAN) {
                 if (mx <= v) {
                     
                     return false;
                 }
             }
             if (kind == SqlKind.GREATER_THAN_OR_EQUAL) {
                 if (mx < v) {
                     
                     return false;
                 }
             }
             if (kind == SqlKind.EQUALS) {
                 if (mx < v || mn > v) {
                     
                     return false;
                 }
             }

        return true;
    }

    private Long convertToLong(Comparable val) {
        if (val instanceof Number) return ((Number) val).longValue();
        if (val instanceof java.util.Calendar) return ((java.util.Calendar) val).getTimeInMillis();
        if (val instanceof org.apache.calcite.util.TimestampString) return ((org.apache.calcite.util.TimestampString) val).getMillisSinceEpoch();
        return null;
    }

    @Override
    public void close() throws Exception {
        for (Scanner s : scanners) {
            try { s.close(); } catch (Exception e) {}
        }
    }

    public int getScannedFilesCount() {
        return scanners.size();
    }

    public List<RexNode> getConditions() {
        return conditions;
    }
}
