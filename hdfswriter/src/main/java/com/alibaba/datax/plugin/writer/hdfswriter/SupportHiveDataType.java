package com.alibaba.datax.plugin.writer.hdfswriter;

public enum SupportHiveDataType {
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,

    TIMESTAMP,
    DATE,

    STRING,
    VARCHAR,
    CHAR,

    BOOLEAN;

    /**
     * 对decimal做加工
     */
    public static SupportHiveDataType valOf(String name){
        if (name != null) {
            if (name.trim().toUpperCase().startsWith("DECIMAL")){
                return DECIMAL;
            }
        }
        return valueOf(name);
    }
}
