package com.querifylabs.dbcourse.schema;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ParquetSchema extends AbstractSchema {
    private final Map<String, Schema> subSchemaMap;

    public ParquetSchema(String rootPath, RelDataTypeFactory relDataTypeFactory) {
        this.subSchemaMap = new HashMap<>();

        File root = new File(rootPath);
        if (root.isDirectory()) {
            File[] schemaFolders = root.listFiles(File::isDirectory);
            if (schemaFolders != null) {
                for (File schemaFolder : schemaFolders) {
                    String schemaName = schemaFolder.getName();
                    subSchemaMap.put(schemaName, new ParquetSubSchema(schemaFolder.getPath(), relDataTypeFactory));
                }
            }
        }
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return subSchemaMap;
    }
}
