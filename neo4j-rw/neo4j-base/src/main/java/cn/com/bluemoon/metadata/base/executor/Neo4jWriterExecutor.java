package cn.com.bluemoon.metadata.base.executor;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.factory.NodeSyncServiceFactory;
import cn.com.bluemoon.metadata.base.factory.RelationSyncServiceFactory;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;

/**
 * 
 * @Date 2021/1/6 10:41
 * @Version 1.0
 */
public class Neo4jWriterExecutor {



    /**
     * 批量处理
     * @param driver
     * @param createTypeConfig
     * @param allRecs
     */
    public static void batchExecute(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) throws ClassNotFoundException {

        if (createTypeConfig.getCreateType().equals(Neo4jCypherConstants.CREATE_TYPE_ON_NODE)) {
            createTypeConfig.setNodeTotalCount(createTypeConfig.getNodeTotalCount() + allRecs.size());
            NodeSyncServiceFactory.getNodeSyncService(createTypeConfig.getMode()).batchSync(driver,createTypeConfig,allRecs);

        }else if (createTypeConfig.getCreateType().equals(Neo4jCypherConstants.CREATE_TYPE_ON_REL)){
            createTypeConfig.setRelTotalCount(createTypeConfig.getRelTotalCount() + allRecs.size());
            RelationSyncServiceFactory.getRelationSyncService().batchSync(driver,createTypeConfig,allRecs);

        }else {
            throw new UnsupportedOperationException("现在不允许创建同步【"+createTypeConfig.getCreateType()+"】类型的作业！");
        }


    }
}
