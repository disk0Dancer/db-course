package com.querifylabs.dbcourse.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeTransforms;

public class SqlBase64DecodeFunction extends SqlFunction {

    public SqlBase64DecodeFunction() {
        super(
                "base64decode",
                org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARBINARY.andThen(SqlTypeTransforms.TO_NULLABLE),
                null,
                OperandTypes.CHARACTER,
                SqlFunctionCategory.STRING
        );
    }
}
