package cn.com.bluemoon.metadata.base.factory;

import cn.com.bluemoon.metadata.base.service.NodeSyncService;
import cn.com.bluemoon.metadata.base.service.RelationSyncService;
import cn.com.bluemoon.metadata.base.service.impl.Neo4jOperatorServiceImpl;
import cn.com.bluemoon.metadata.base.service.impl.RelationSyncServiceImpl;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * 
 * @Date 2021/1/7 16:04
 * @Version 1.0
 */
public class RelationSyncServiceFactory {

    private static final List<RelationSyncService> syncServices = Lists.newArrayList();

    static {
        syncServices.add(new RelationSyncServiceImpl(new Neo4jOperatorServiceImpl()));
    }

    public static RelationSyncService getRelationSyncService() {
        return syncServices.get(0);
    }


}
