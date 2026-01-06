package com.querifylabs.dbcourse.sql;

import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class ExtendedSqlOperatorTable extends ReflectiveSqlOperatorTable {
    private static ExtendedSqlOperatorTable instance;

    public static synchronized ExtendedSqlOperatorTable instance() {
        if (instance == null) {
            instance = new ExtendedSqlOperatorTable();
            instance.init();
        }
        return instance;
    }

    public static final SqlBase64DecodeFunction BASE64DECODE = new SqlBase64DecodeFunction();
}
