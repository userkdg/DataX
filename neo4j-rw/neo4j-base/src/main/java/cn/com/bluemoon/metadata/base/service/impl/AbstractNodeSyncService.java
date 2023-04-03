package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.service.NodeSyncService;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * 
 * @Date 2021/1/8 9:47
 * @Version 1.0
 */
@SuppressWarnings("ALL")
public abstract class AbstractNodeSyncService implements NodeSyncService {

    // 统计指标字段，每天都不一样，不进行比较
    protected static final Set<String> indexField = ImmutableSet.of("tableRows", "avgRowLength", "dataLength", "indexLength", "numFiles", "totalSize", "lastModifiedTime", "lastAccessTime", "partitionCount", "assetStatusOperatorTime");
    protected final Set<String> dateFieldSet = ImmutableSet.of("moiCreateDate", "moiUpdateDate", "createTime", "updateTime", "publishTime", "assetStatusOperatorTime");
    protected final String[] LONG_DATA_KEY = {"columnId", "moiSeq"};
    protected final String[] INTEGER_DATA_KEY = {"dataType", "referRules", "bizLineId", "indexFormat", "dataWarehouse", "indexType", "assetStatus"};
    protected Neo4jOperatorService neo4jOperatorService;
    private Logger logger = LoggerFactory.getLogger(AbstractNodeSyncService.class);


    public AbstractNodeSyncService(Neo4jOperatorService neo4jOperatorService) {
        this.neo4jOperatorService = neo4jOperatorService;
    }

}
