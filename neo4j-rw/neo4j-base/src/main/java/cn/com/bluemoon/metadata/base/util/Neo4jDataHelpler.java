package cn.com.bluemoon.metadata.base.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;

import java.util.List;
import java.util.Map;

/**
 * 
 * @Date 2021/1/6 13:55
 * @Version 1.0
 */
public class Neo4jDataHelpler {

    /**
     * 查询节点
     * @param cypher
     * @param session
     * @param parameters
     * @return
     */
    public static List<Map<String,Object>> queryNodeForList(String cypher, Session session, Map<String, Object> parameters) {
        List<Map<String,Object>> result = Lists.newArrayList();
        Result statementResult = session.run(cypher, parameters);
        List<Record> list = statementResult.list();
        if (CollectionUtils.isNotEmpty(list)) {
            for (Record record : list) {
                Map<String,Object> map = Maps.newHashMap();
                List<String> keys = record.keys();
                for (String key : keys) {
                    Node node = record.get(key).asNode();
                    for (String s : node.keys()) {
                        map.put(s,node.get(s).asObject());
                    }
                    result.add(map);
                }
            }
        }
        return result;
    }


}
