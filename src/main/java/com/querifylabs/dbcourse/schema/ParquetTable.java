package com.querifylabs.dbcourse.schema;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetTable extends AbstractTable implements TranslatableTable {
    private final String tablePath;
    private final String tableName;
    private final String schemaName;
    private RelDataType rowType;

    public ParquetTable(String tablePath, RelDataTypeFactory relDataTypeFactory) {
        this.tablePath = tablePath;

        File tableDir = new File(tablePath);
        this.tableName = tableDir.getName();
        this.schemaName = tableDir.getParentFile().getName();
    }

    public String schema() {
        return schemaName;
    }

    public String name() {
        return tableName;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return LogicalTableScan.create(toRelContext.getCluster(), relOptTable, List.of());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        if (rowType == null) {
            rowType = buildRowType(relDataTypeFactory);
        }
        return rowType;
    }

    private RelDataType buildRowType(RelDataTypeFactory typeFactory) {
        try {
            MessageType parquetSchema = readParquetSchema();
            List<RelDataType> fieldTypes = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();

            for (Type field : parquetSchema.getFields()) {
                fieldNames.add(field.getName());
                fieldTypes.add(convertParquetTypeToSqlType(field, typeFactory));
            }

            return typeFactory.createStructType(fieldTypes, fieldNames);
        } catch (IOException e) {
            throw new RuntimeException("failed to read: ", e);
        }
    }

    private MessageType readParquetSchema() throws IOException {
        File tableDir = new File(tablePath);
        File[] parquetFiles = tableDir.listFiles((dir, name) -> name.endsWith(".parquet"));

        if (parquetFiles == null || parquetFiles.length == 0) {
            throw new RuntimeException("not foud: " + tablePath);
        }

        // Use LocalInputFile for direct file access without Hadoop security
        LocalInputFile inputFile = new LocalInputFile(parquetFiles[0].toPath());
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        reader.close();

        return schema;
    }

    private RelDataType convertParquetTypeToSqlType(Type parquetType, RelDataTypeFactory typeFactory) {
        if (parquetType.isPrimitive()) {
            PrimitiveType primitiveType = parquetType.asPrimitiveType();
            org.apache.parquet.schema.Type.Repetition repetition = parquetType.getRepetition();
            boolean nullable = repetition == org.apache.parquet.schema.Type.Repetition.OPTIONAL;

            LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

            switch (primitiveType.getPrimitiveTypeName()) {
                case BINARY:
                    if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                        return typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.VARCHAR, -1), nullable);
                    }
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.VARBINARY), nullable);
                case INT32:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.INTEGER), nullable);
                case INT64:
                    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        return typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6), nullable);
                    }
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.BIGINT), nullable);
                case DOUBLE:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.DOUBLE), nullable);
                case FLOAT:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.REAL), nullable);
                case BOOLEAN:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullable);
                default:
                    return typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.VARCHAR), nullable);
            }
        }

        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    }
}

