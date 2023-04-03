package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.enums.ModeEnums;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 
 * @Date 2021/1/6 14:26
 * @Version 1.0
 */
public class CreateNodeSyncServiceImpl extends AbstractNodeSyncService {

    public CreateNodeSyncServiceImpl(Neo4jOperatorService neo4jOperatorService) {
        super(neo4jOperatorService);
    }

    @Override
    public boolean support(String mode) {
        return ModeEnums.CREATE.getCode().equals(mode);
    }

    /**
     * 直接创建即可
     *
     * @param driver
     * @param createTypeConfig
     * @param allRecs
     */
    @Override
    public void batchSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        neo4jOperatorService.batchInsert(driver,createTypeConfig,allRecs);
    }
}
