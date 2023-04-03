package cn.com.bluemoon.metadata.base.service;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;

/**
 * 
 * @Date 2021/1/7 9:29
 * @Version 1.0
 */
public interface Neo4jOperatorService {

    /**
     * 执行cypher语句
     *
     * @param driver
     * @param cypher
     * @param param
     */
    void run(Driver driver, String cypher, Map<String, Object> param);


    /**
     * 批量插入
     *
     * @param driver
     * @param createTypeConfig
     * @param allRecs
     */
    void batchInsert(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs);

    /**
     * 重置最新不为0
     *
     * @param driver
     * @param moiDataIds
     */
    void resetNewestToZero(Driver driver, List<String> moiDataIds);


    /**
     * 根据moiDataId查询数据
     *
     * @param driver
     * @param moiDataIds
     * @return
     */
    List<Map<String, Object>> queryDataByMoiDataIds(Driver driver, List<String> moiDataIds);


    /**
     * 根据guid查询数据
     *
     * @param driver
     * @param guids
     * @return
     */
    List<Map<String, Object>> queryDataByGuid(Driver driver, List<String> guids);

    /**
     * 积极的同步策略
     *
     * @param driver
     * @param positiveIncrement
     */
    void positiveSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> positiveIncrement);

    /**
     * 保守的同步策略
     *  保守同步的策略就是不删除原来的节点
     *  然后在原来的节点上进行修改
     * @param driver
     * @param context
     * @param positiveIncrement
     */
    void negtiveSync(Driver driver, CreateTypeConfig context, List<Map<String, Object>> positiveIncrement);
}
