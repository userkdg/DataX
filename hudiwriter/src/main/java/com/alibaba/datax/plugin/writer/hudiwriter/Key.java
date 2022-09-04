package com.alibaba.datax.plugin.writer.hudiwriter;

public class Key {
    public static final String HUDI_TABLE_NAME = "tableName";
    public static final String HUDI_TABLE_PATH = "tablePath";
    public static final String HUDI_PRIMARY_KEY = "primaryKey";
    public static final String HUDI_PARTITION_FIELDS = "partitionFields";
    public static final String HUDI_TABLE_TYPE = "tableType";
    public static final String HUDI_BATCH_SIZE = "batchSize";
    public static final String HUDI_WRITE_OPTION = "writeOption";
    public static final String HUDI_COLUMN = "column";

    public static final String HUDI_WRITE_OPTION_INSERT = "insert";
    public static final String HUDI_WRITE_OPTION_BULK_INSERT = "bulk_insert";
    public static final String HUDI_WRITE_OPTION_UPSERT = "upsert";

    public static final String HUDI_WRITE_TYPE_COW = "cow";
    public static final String HUDI_WRITE_TYPE_MOR = "mor";

    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
}
