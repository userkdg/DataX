package cn.com.bluemoon.metadata.base.service;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;

/**
 * 实体节点同步服务
 *
 * 
 * @Date 2021/1/6 11:17
 * @Version 1.0
 */
public interface NodeSyncService {

    /**
     * 模式是否支持该同步服务
     * @param mode
     * @return
     */
    boolean support(String mode);

    /**
     * 将数据批量导入到图形数据库中
     * @param driver
     * @param createTypeConfig
     * @param allRecs
     */
    void batchSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs);

}
