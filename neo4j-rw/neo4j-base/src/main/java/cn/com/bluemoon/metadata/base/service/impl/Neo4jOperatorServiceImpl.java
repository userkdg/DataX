package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.ICypherGenerator;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.util.Neo4jDataHelpler;
import com.google.common.collect.Maps;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;

/**
 * 
 * @Date 2021/1/7 9:30
 * @Version 1.0
 */
public class Neo4jOperatorServiceImpl implements Neo4jOperatorService {

    private final String DELETE_NODE_REL = "match (n:Neo4jNodeBaseEntity)-[r]-(l:Neo4jNodeBaseEntity) where n.guid in {guids} delete r";
    private final String DELETE_NODE = "match (n:Neo4jNodeBaseEntity) where n.guid in {guids} delete n";


    @Override
    public void run(Driver driver, String cypher, Map<String, Object> param) {
        try (Session session = driver.session()) {
            session.run(cypher, param);
        }
    }

    @Override
    public void batchInsert(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        ICypherGenerator cypherGenerator = new NodeCypherGenerator();
        String cypher = cypherGenerator.generateCreateCql(createTypeConfig);
        Map<String, Object> param = Maps.newHashMap();
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, allRecs);
        try (Session session = driver.session()) {
            session.run(cypher, param);
        }
    }

    @Override
    public void resetNewestToZero(Driver driver, List<String> moiDataIds) {
        if (moiDataIds.isEmpty()) return;
        String cypher = "match (n:Neo4jNodeBaseEntity) where n.moiDataId in {moiDataIds} set n.moiIsNewestVersion=0 , n.moiIsDelete = 1 return count(n)";
        Map<String, Object> param = Maps.newHashMap();
        param.put("moiDataIds", moiDataIds);
        try (Session session = driver.session()) {
            session.run(cypher, param);
        }
    }

    @Override
    public List<Map<String, Object>> queryDataByMoiDataIds(Driver driver, List<String> moiDataIds) {
        // 查询图库中是否存在
        Map<String, Object> param = Maps.newHashMap();
        param.put("moiDataIds", moiDataIds);
        String cql = "match (n:Neo4jNodeBaseEntity) where n.moiIsDelete=0 and n.moiIsNewestVersion=1 and n.moiDataId in {moiDataIds} return n";

        List<Map<String, Object>> existing;
        try (Session session = driver.session()) {
            existing = Neo4jDataHelpler.queryNodeForList(cql, session, param);
        }
        return existing;
    }

    @Override
    public List<Map<String, Object>> queryDataByGuid(Driver driver, List<String> guids) {
        // 查询图库中是否存在
        Map<String, Object> param = Maps.newHashMap();
        param.put("guids", guids);
        String cql = "match (n:Neo4jNodeBaseEntity) where n.moiIsDelete=0 and n.moiIsNewestVersion=1 and n.guid in {guids} return n";
        List<Map<String, Object>> existing;
        try (Session session = driver.session()) {
            existing = Neo4jDataHelpler.queryNodeForList(cql, session, param);
        }
        return existing;
    }

    @Override
    public void positiveSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> positiveIncrement) {
        /*for (Map<String, Object> positive : positiveIncrement) {
            positive.put("collectType", CollectTypeEnum.UPDATE.getCode());
        }
        // 首先删除节点以及节点之间的关系,然后新增
        List<String> guidList = positiveIncrement.stream().map(x -> x.get("guid").toString()).collect(Collectors.toList());
        Map<String, Object> param = new HashMap<>();
        param.put("guids", guidList);
        run(driver, DELETE_NODE_REL, param);
        run(driver, DELETE_NODE, param);
        batchInsert(driver, createTypeConfig, positiveIncrement);*/
    }

    @Override
    public void negtiveSync(Driver driver, CreateTypeConfig context, List<Map<String, Object>> negtiveIncrement) {
        /*for (Map<String, Object> positive : negtiveIncrement) {
            positive.put("collectType", CollectTypeEnum.UPDATE.getCode());
        }

        List<String> guidList = negtiveIncrement.stream().map(x -> x.get("guid").toString()).collect(Collectors.toList());

        List<Map<String, Object>> maps = queryDataByGuid(driver, guidList);
        Map<Object, Map<String, Object>> guidToMap = maps.stream().collect(Collectors.toMap(x -> x.get("guid"), y -> y, (x, y) -> x));


        NodeCypherGenerator nodeCypherGenerator = new NodeCypherGenerator();
        Map<String, Object> param = Maps.newHashMap();

        for (Map<String, Object> negtiveMap : negtiveIncrement) {
            Map<String, Object> map = guidToMap.get(negtiveMap.get("guid").toString());
            negtiveMap.put("moiDataId", map.get("moiDataId"));
            negtiveMap.put("moiDataIdPath", map.get("moiDataIdPath"));
            negtiveMap.put("moiParentDataId", map.get("moiParentDataId"));
            negtiveMap.put("moiParentDataIdPath", map.get("moiParentDataIdPath"));
            negtiveMap.put("moiDataVersion", 1);
            negtiveMap.put("moiIsDelete",0);
            negtiveMap.put("moiIsNewestVersion",1);
        }
        String updateCql = nodeCypherGenerator.generateUpdateCql(context);
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, negtiveIncrement);
        run(driver, updateCql, param);*/

    }
}
