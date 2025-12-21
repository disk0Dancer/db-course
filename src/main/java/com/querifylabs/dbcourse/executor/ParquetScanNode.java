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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ParquetScanNode extends BaseExecutionNode {
    private final ParquetTable table;
    private final List<Integer> projectedColumns;
    private Scanner scanner;
    private Iterator<? extends ScanTask> scanTaskIterator;
    private ArrowReader currentReader;

    public ParquetScanNode(ExecutionContext executionContext, ParquetTable table, List<Integer> projectedColumns) {
        super(executionContext);
        this.table = table;
        this.projectedColumns = projectedColumns;
    }

    @Override
    public DataPage getNextPage() throws ExecutorException {
        if (scanner == null) {
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
        List<String> filePaths = new ArrayList<>();
        for (File f : table.getFiles()) {
            filePaths.add(f.getAbsolutePath());
        }

        if (filePaths.isEmpty()) {
            scanTaskIterator = new ArrayList<ScanTask>().iterator();
            return;
        }

        // System.out.println("Reading from " + filePaths.get(0));

        DatasetFactory dsFactory = new FileSystemDatasetFactory(
            executionContext.getAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, table.getFiles().get(0).getParentFile().getAbsoluteFile().toURI().toString());

        Schema s = null;
        try {
            s = dsFactory.inspect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

        scanner = dataset.newScan(builder.build());
        scanTaskIterator = scanner.scan().iterator();
    }

    @Override
    public void close() throws Exception {
        if (scanner != null) {
            scanner.close();
        }
    }
}
