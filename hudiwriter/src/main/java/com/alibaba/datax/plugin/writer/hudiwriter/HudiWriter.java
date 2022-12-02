package com.alibaba.datax.plugin.writer.hudiwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.datax.plugin.writer.hudiwriter.HudiWriterErrorCode.HUDI_ERROR_TABLE;
import static com.alibaba.datax.plugin.writer.hudiwriter.HudiWriterErrorCode.HUDI_PARAM_LOST;
import static com.alibaba.datax.plugin.writer.hudiwriter.Key.*;

/**
 * Created by david.dong on 22-8-21.
 */
public class HudiWriter extends Writer {
    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private String primaryKey;
        private String partitionFields;
        private String writeOption;
        private int batchSize;
        private Configuration sliceConfig;
        private List<Configuration> columnsList;

        private List<String> partitionList;

        Schema avroSchema;

        private HoodieJavaWriteClient<HoodieAvroPayload> client;

        @Override
        public void init() {
            //获取与本task相关的配置
            this.sliceConfig = super.getPluginJobConf();
            String tableName = sliceConfig.getNecessaryValue(Key.HUDI_TABLE_NAME, HUDI_ERROR_TABLE);
            String tablePath = sliceConfig.getNecessaryValue(Key.HUDI_TABLE_PATH, HUDI_PARAM_LOST);
            String tableType = sliceConfig.getNecessaryValue(Key.HUDI_TABLE_TYPE, HUDI_PARAM_LOST);
            primaryKey = sliceConfig.getNecessaryValue(Key.HUDI_PRIMARY_KEY, HUDI_PARAM_LOST);
            partitionFields = sliceConfig.getString(Key.HUDI_PARTITION_FIELDS);
            writeOption = sliceConfig.getNecessaryValue(Key.HUDI_WRITE_OPTION, HUDI_PARAM_LOST);
            columnsList = sliceConfig.getListConfiguration(Key.HUDI_COLUMN);
            batchSize = sliceConfig.getInt(HUDI_BATCH_SIZE);

            partitionList = StringUtils.isEmpty(partitionFields) ? new ArrayList<>() : Arrays.asList(partitionFields.split(","));

            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            try {
                //是否有Kerberos认证
                Boolean haveKerberos = sliceConfig.getBool(HAVE_KERBEROS, false);
                if(haveKerberos){
                    String kerberosKeytabFilePath = sliceConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
                    String kerberosPrincipal = sliceConfig.getString(Key.KERBEROS_PRINCIPAL);
                    hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
                    this.kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
                }
                //初始化HDFS
                Path path = new Path(tablePath);
                FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
                if (!fs.exists(path)) {
                    HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(HUDI_WRITE_TYPE_MOR.equals(tableType) ? HoodieTableType.MERGE_ON_READ : HoodieTableType.COPY_ON_WRITE)
                        .setTableName(tableName)
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .initTable(hadoopConf, tablePath);
                }
            } catch (IOException e) {
                LOG.error(ExceptionUtils.getStackTrace(e));
            }
            Map<String, String> typeMap = new HashMap<String, String>(){{
                put("date", "string");
                put("datetime", "string");
                put("bigint", "long");
            }};
            JSONArray fields = new JSONArray();
            for (Configuration columnConfig : columnsList) {
                JSONObject confObject = new JSONObject();
                confObject.put("name", columnConfig.getString("name"));
                String configType = columnConfig.getString("type");
                JSONArray unionsType = new JSONArray();
                unionsType.add("null");
                unionsType.add(typeMap.getOrDefault(configType, configType));
                confObject.put("type", unionsType);
                fields.add(confObject);
            }

            JSONObject schemaObject = new JSONObject();
            schemaObject.put("type", "record");
            schemaObject.put("name", "triprec");
            schemaObject.put("fields", fields);
            String schemaStr = schemaObject.toJSONString();
            System.out.println("===============hudi schema===========");
            System.out.println(schemaStr);

            avroSchema = new Schema.Parser().parse(schemaStr);

            // Create the write client to write some records in
            HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(schemaStr).withParallelism(2, 2)
                .withDeleteParallelism(2).forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
            client =
                new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            AtomicLong counter = new AtomicLong(0);
            List<HoodieRecord<HoodieAvroPayload>> writeRecords = new ArrayList<>();
            while ((record = recordReceiver.getFromReader()) != null) {
                GenericRecord row = new GenericData.Record(avroSchema);
                for (int i=0; i<columnsList.size(); i++) {
                    Configuration configuration = columnsList.get(i);
                    String columnName = configuration.getString("name");
                    String columnType = configuration.getString("type");
                    Column column = record.getColumn(i);
                    Object rawData = column.getRawData();
                    if (rawData == null) {
                        row.put(columnName, null);
                        continue;
                    }
                    switch (columnType) {
                        case "int":
                            row.put(columnName, Integer.parseInt(rawData.toString()));
                            break;
                        case "bigint":
                        case "long":
                            row.put(columnName, Long.parseLong(rawData.toString()));
                            break;
                        case "float":
                            row.put(columnName, Float.parseFloat(rawData.toString()));
                            break;
                        case "double":
                            row.put(columnName, Double.parseDouble(rawData.toString()));
                            break;
                        case "date":
                            row.put(columnName, dateFormat.format(rawData));
                            break;
                        case "datetime":
                            row.put(columnName, dateTimeFormat.format(rawData));
                            break;
                        case "string":
                        default:
                            row.put(columnName, rawData.toString());
                    }
                }
                String partitionPath = "";
                if (!partitionList.isEmpty()) {
                    List<Object> values = partitionList.stream().map(row::get).collect(Collectors.toList());
                    partitionPath = StringUtils.join(values, "/");
                }
                HoodieKey key = new HoodieKey(row.get(primaryKey).toString(), partitionPath);
                HoodieRecord<HoodieAvroPayload> hoodieAvroPayload = new HoodieRecord<>(key, new HoodieAvroPayload(Option.of(row)));
                writeRecords.add(hoodieAvroPayload);
                long num = counter.incrementAndGet();

                if (num >= batchSize) {
                    flushCache(writeRecords);
                    writeRecords.clear();
                    counter.set(0L);
                }
            }
            if (!writeRecords.isEmpty()) {
                flushCache(writeRecords);
            }
        }

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, org.apache.hadoop.conf.Configuration hadoopConf){
            if(StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)){
                UserGroupInformation.setConfiguration(hadoopConf);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                } catch (Exception e) {
                    String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                            kerberosKeytabFilePath, kerberosPrincipal);
                    LOG.error(message);
                    throw DataXException.asDataXException(HudiWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
                }
            }
        }

        private void flushCache(List<HoodieRecord<HoodieAvroPayload>> writeRecords) {
            String commitTime = client.startCommit();
            LOG.info("Starting commit " + commitTime);
            switch (writeOption) {
                case HUDI_WRITE_OPTION_INSERT:
                    client.insert(writeRecords, commitTime);
                    break;
                case HUDI_WRITE_OPTION_BULK_INSERT:
                    client.bulkInsert(writeRecords, commitTime);
                    break;
                case HUDI_WRITE_OPTION_UPSERT:
                    client.upsert(writeRecords, commitTime);
                    break;
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            if (client!=null) {
                client.close();
            }
        }
    }
}
