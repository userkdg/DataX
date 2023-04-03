package cn.com.bluemoon.reader.neo4j;

import cn.com.bluemoon.metadata.base.util.JavaDriverFactory;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.util.Pair;

import java.sql.Date;
import java.time.*;
import java.util.List;

/**
 * @author Lilike on 2022/4/22
 */
@Slf4j
public class Neo4jWriterHelper {


    /**
     * 读取图库里面的数据，并将数据写入到通道
     *
     * @param recordSender
     * @param taskPluginCollector
     * @param queryCql
     */
    public static void read(RecordSender recordSender, TaskPluginCollector taskPluginCollector, String queryCql) {

        Session session = JavaDriverFactory.getDriver().session();
        Query query = new Query(queryCql);
        Result result = session.run(query);
        if (!result.hasNext()) {
            log.warn("没有读取到数据!");
            return;
        }
        for (Record record : result.list()) {
            com.alibaba.datax.common.element.Record dataXRecord = buildDataXRecord(recordSender, record);
            recordSender.sendToWriter(dataXRecord);
        }
    }

    private static com.alibaba.datax.common.element.Record buildDataXRecord(RecordSender recordSender, Record record) {
        com.alibaba.datax.common.element.Record dataXRecord = recordSender.createRecord();
        List<Pair<String, Value>> fields = record.fields();
        for (Pair<String, Value> field : fields) {
            Value value = field.value();
            TypeRepresentation type = (TypeRepresentation) value.type();
            TypeConstructor constructor = type.constructor();
            if (constructor.equals(TypeConstructor.STRING)) {
                dataXRecord.addColumn(new StringColumn(value.asString()));
            } else if (constructor.equals(TypeConstructor.BOOLEAN)) {
                dataXRecord.addColumn(new BoolColumn(value.asBoolean()));
            } else if (constructor.equals(TypeConstructor.NUMBER)) {
                dataXRecord.addColumn(new DoubleColumn(value.asNumber().doubleValue()));
            } else if (constructor.equals(TypeConstructor.INTEGER)) {
                dataXRecord.addColumn(new LongColumn(value.asLong()));
            } else if (constructor.equals(TypeConstructor.FLOAT)) {
                dataXRecord.addColumn(new DoubleColumn(value.asDouble()));
            } else if (constructor.equals(TypeConstructor.DATE_TIME)) {
                ZonedDateTime localTime = value.asZonedDateTime();
                dataXRecord.addColumn(new DateColumn(Date.from(localTime.toInstant())));
            } else if (constructor.equals(TypeConstructor.DATE)) {
                LocalDate localDate = value.asLocalDate();
                dataXRecord.addColumn(new DateColumn(Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant())));
            } else if (constructor.equals(TypeConstructor.NULL)) {
                dataXRecord.addColumn(new StringColumn(null));
            } else {
                throw DataXException
                        .asDataXException(
                                Neo4jReaderErrorCode.REQUIRED_VALUE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s],  字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                        field.key(),
                                        constructor.toString()));
            }
        }


        return dataXRecord;
    }

}
