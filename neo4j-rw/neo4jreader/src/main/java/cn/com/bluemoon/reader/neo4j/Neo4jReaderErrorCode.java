package cn.com.bluemoon.reader.neo4j;


import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @file Neo4jWriterErrorCode.java
 * 
 * @date 2021/1/6
 */
public enum Neo4jReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("Neo4jReader-00", "暂时不支持该数据类型，请联系开发人员.");

    private final String code;
    private final String description;

    private Neo4jReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}