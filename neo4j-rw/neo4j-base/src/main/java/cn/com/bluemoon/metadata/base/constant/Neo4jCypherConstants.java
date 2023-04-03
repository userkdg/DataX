package cn.com.bluemoon.metadata.base.constant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @Date 2021/1/6 14:42
 * @Version 1.0
 */
public class Neo4jCypherConstants {

    public static final Logger log = LoggerFactory.getLogger(Neo4jCypherConstants.class);

    public final static String CREATE_TYPE_ON_NODE = "NODE";
    public final static String CREATE_TYPE_ON_REL = "RELATIONSHIP";

    public final static String DEFAULT_BATCH_KEY = "batches";

    public final static List<String> BASE_NODE_COLUMNS = new ArrayList<>();
    public final static List<String> BASE_RELATION_COLUMNS = new ArrayList<>();

    static {
//        Class<?> aClass = null;
//        Class<?> bClass = null;
//        try {
//            aClass = Class.forName("cn.com.bluemoon.metadata.neo4j.dal.neo4j.base.Neo4jNodeBaseEntity");
//            bClass = Class.forName("cn.com.bluemoon.metadata.neo4j.dal.neo4j.base.Neo4jRelationshipsBaseEntity");
//        } catch (ClassNotFoundException e) {
//            log.error("获取到基础类的属性失败！");
//        }
//        List<Field> fields = ReflectionUtils.getFields(aClass);
//        BASE_NODE_COLUMNS.addAll(fields.stream().map(x -> x.getName()).collect(Collectors.toList()));
//        fields = ReflectionUtils.getFields(bClass);
//        BASE_RELATION_COLUMNS.addAll(fields.stream().map(x -> x.getName()).collect(Collectors.toList()));
    }


}

