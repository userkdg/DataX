package com.alibaba.datax.plugin.writer.hudiwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
        private String writeOption;
        private Configuration sliceConfig;
        private List<Configuration> columnsList;

        Schema avroSchema;

        private HoodieJavaWriteClient<HoodieAvroPayload> client;

        @Override
        public void init() throws IOException {
            //获取与本task相关的配置
            this.sliceConfig = super.getPluginJobConf();
            String tableName = sliceConfig.getNecessaryValue(Key.HUDI_TABLE_NAME, HUDI_ERROR_TABLE);
            String tablePath = sliceConfig.getNecessaryValue(Key.HUDI_TABLE_PATH, HUDI_PARAM_LOST);
            String tableType = sliceConfig.getNecessaryValue(Key.HUDI_TABLE_TYPE, HUDI_PARAM_LOST);
            primaryKey = sliceConfig.getNecessaryValue(Key.HUDI_PRIMARY_KEY, HUDI_PARAM_LOST);
            writeOption = sliceConfig.getNecessaryValue(Key.HUDI_WRITE_OPTION, HUDI_PARAM_LOST);
            columnsList = sliceConfig.getListConfiguration(Key.HUDI_COLUMN);

            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            // initialize the table, if not done already
            Path path = new Path(tablePath);
            FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
            if (!fs.exists(path)) {
                HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(HUDI_WRITE_TYPE_MOR.equals(tableType) ? HoodieTableType.MERGE_ON_READ : HoodieTableType.COPY_ON_WRITE)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(hadoopConf, tablePath);
            }

            JSONArray fields = new JSONArray();
            for (Configuration columnConfig : columnsList) {
                JSONObject confObject = new JSONObject();
                confObject.put("name", columnConfig.getString("name"));
                confObject.put("type", columnConfig.getString("type"));
                fields.add(confObject);
            }

            JSONObject schemaObject = new JSONObject();
            schemaObject.put("type", "record");
            schemaObject.put("name", "triprec");
            schemaObject.put("fields", fields);
            String schemaStr = schemaObject.toJSONString();

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
            int batchSize = 100;
            AtomicLong counter = new AtomicLong(0);
            List<HoodieRecord<HoodieAvroPayload>> writeRecords = new ArrayList<>();
            while ((record = recordReceiver.getFromReader()) != null) {
                GenericRecord row = new GenericData.Record(avroSchema);
                for (int i=0; i<columnsList.size(); i++) {
                    Configuration configuration = columnsList.get(i);
                    String columnName = configuration.getString("name");
                    String columnType = configuration.getString("type");
                    Column column = record.getColumn(i);
                    Object data = column.getRawData();
                    switch (columnType) {
                        case "int":
                            row.put(columnName, Integer.parseInt(data.toString()));
                            break;
                        case "float":
                            row.put(columnName, Float.parseFloat(data.toString()));
                            break;
                        case "string":
                            row.put(columnName, data.toString());
                            break;
                        case "double":
                            row.put(columnName, Double.parseDouble(data.toString()));
                            break;
                    }
                }
                HoodieKey key = new HoodieKey(row.get(primaryKey).toString(), "");
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
