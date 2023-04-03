package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import cn.com.bluemoon.metadata.base.service.RelationSyncService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.Driver;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 同步关系
 *
 * 
 * @Date 2021/1/7 15:24
 * @Version 1.0
 */
@SuppressWarnings("ALL")
@Slf4j
public class RelationSyncServiceImpl implements RelationSyncService {

    private Neo4jOperatorService neo4jOperatorService;

    public RelationSyncServiceImpl(Neo4jOperatorService neo4jOperatorService) {
        this.neo4jOperatorService = neo4jOperatorService;
    }

    @Override
    public void batchSync(Driver driver, CreateTypeConfig createTypeConfig, List<Map<String, Object>> allRecs) {
        allRecs = allRecs.stream().filter(x -> x.get("sourceGuid") != null && x.get("targetGuid") != null).collect(Collectors.toList());
        // TODO: 2023/4/3 业务加工
        /*for (Map<String, Object> map : allRecs) {
            replaceEnvAndCatalog(createTypeConfig, map,"sourceGuid");
            replaceEnvAndCatalog(createTypeConfig, map,"targetGuid");
        }

        Map<String, String> guidToMoiDataId = getGuidToMoiDataId(driver, allRecs);
        Date currentDate = new Date();
        allRecs.forEach(map -> {
            if (StringUtils.isNotEmpty(createTypeConfig.getRelationType())) {
                map.put("relationType", createTypeConfig.getRelationType());
            }else {
                map.put("relationType", RelationType.RT_AGGREGATION_COMPOSITION);
            }
            map.put("relationshipIsDelete",0);
            map.put("moiCreateDate",Long.parseLong(DateUtils.format(currentDate,DateUtils.DATE_TIME_FORMAT_YYYYMMDDHHMISS)));
            map.put("sourceDataId",guidToMoiDataId.get(map.get("sourceGuid")));
            map.put("targetDataId",guidToMoiDataId.get(map.get("targetGuid")));
            map.put("sourceGuid",null);
            map.put("targetGuid",null);
        });*/
        String cypher = new RelCypherGenerator().generateCreateCql(createTypeConfig);
        Map<String, Object> param = new HashMap<>();
        param.put(Neo4jCypherConstants.DEFAULT_BATCH_KEY, allRecs);
        log.info("正在做关系同步....本次关系同步{}->{},同步关系结果:同步关系{}条,当前已同步{}条.", createTypeConfig.getSrcLabel(), createTypeConfig.getTargetLabel(), allRecs.size(), createTypeConfig.getRelTotalCount());
        neo4jOperatorService.run(driver, cypher, param);
    }

    private void replaceEnvAndCatalog(CreateTypeConfig createTypeConfig, Map<String, Object> map, String key) {
        String value = map.get(key).toString();
        if (value.contains("{{envCn}}")) {
            value = value.replaceAll("\\{\\{envCn\\}\\}", createTypeConfig.getEnvCn());
        }
        if (value.contains("{{catalogCn}}")) {
            value = value.replaceAll("\\{\\{catalogCn\\}\\}", createTypeConfig.getCatalogCN());
        }
        map.put(key, value);
    }

    private Map<String, String> getGuidToMoiDataId(Driver driver, List<Map<String, Object>> allRecs) {
        Set<String> guidSets = Sets.newHashSet();
        allRecs.stream().forEach(x -> {
            guidSets.add(x.get("sourceGuid").toString());
            guidSets.add(x.get("targetGuid").toString());
        });

        Set<String> notInCacheSets = Sets.newHashSet();
        Map<String, String> guid2MoiDataId = Maps.newHashMap();
        for (String guidSet : guidSets) {
            String ifPresent = LoadingCacheHolder.relationCache.getIfPresent(guidSet);
            if (StringUtils.isNotEmpty(ifPresent)) {
                guid2MoiDataId.put(guidSet, ifPresent);
            } else {
                notInCacheSets.add(guidSet);
            }
        }

        List<Map<String, Object>> maps = neo4jOperatorService.queryDataByGuid(driver, new ArrayList<>(notInCacheSets));
        for (Map<String, Object> map : maps) {
            try {
                guid2MoiDataId.put(map.get("guid").toString(), map.get("moiDataId").toString());
                LoadingCacheHolder.relationCache.put(map.get("guid").toString(), map.get("moiDataId").toString());
            } catch (NullPointerException ex) {
                log.info("出现空指针异常，结果是：{}", map);
            }
        }
        return guid2MoiDataId;
    }

    public static final class LoadingCacheHolder {
        public static final Cache<String, String> relationCache = CacheBuilder.newBuilder()
                .initialCapacity(100)
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }


}
