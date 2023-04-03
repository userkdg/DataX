package cn.com.bluemoon.metadata.base.service;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;

/**
 * 
 * @Date 2021/1/7 15:22
 * @Version 1.0
 */
public interface RelationSyncService {

    /**
     * 批量更新关系
     * @param driver
     * @param createTypeConfig
     * @param allRecs
     */
    void batchSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs);

}
