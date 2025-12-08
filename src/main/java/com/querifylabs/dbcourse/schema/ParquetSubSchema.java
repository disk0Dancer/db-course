package com.querifylabs.dbcourse.schema;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ParquetSubSchema extends AbstractSchema {
    private final Map<String, Table> tables;

    public ParquetSubSchema(String schemaPath, RelDataTypeFactory relDataTypeFactory) {
        this.tables = new HashMap<>();

        File schemaDir = new File(schemaPath);
        if (schemaDir.isDirectory()) {
            File[] tableFolders = schemaDir.listFiles(File::isDirectory);
            if (tableFolders != null) {
                for (File tableFolder : tableFolders) {
                    String tableName = tableFolder.getName();
                    tables.put(tableName, new ParquetTable(tableFolder.getPath(), relDataTypeFactory));
                }
            }
        }
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }
}

