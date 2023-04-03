package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.enums.ModeEnums;
import cn.com.bluemoon.metadata.base.service.Neo4jOperatorService;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 
 * @Date 2021/1/6 11:19
 * @Version 1.0
 */
public class CompareNodeSyncServiceImpl extends AbstractNodeSyncService {

    private static final Logger log = LoggerFactory.getLogger(CompareNodeSyncServiceImpl.class);

    public CompareNodeSyncServiceImpl(Neo4jOperatorService neo4jOperatorService) {
        super(neo4jOperatorService);
    }


    @Override
    public boolean support(String mode) {
        return ModeEnums.COMPARE.getCode().equals(mode);
    }

    /**
     * 进行比较后选择插入或是不插入
     *
     * @param driver
     * @param context
     * @param allRecs
     */
    @Override
    public void batchSync(Driver driver, CreateTypeConfig context, List<Map<String, Object>> allRecs) {
        // TODO: 2023/4/3 业务加工
        /*allRecs = allRecs.stream().filter(x -> x.get("guid") != null).collect(Collectors.toList());
        this.addDefaultValue(context,allRecs);
        this.addMoiParentDataId(driver,context, allRecs);
        final String COMPARE_ID = "guid";

        // 直接插入
        List<Map<String, Object>> directoryInsert = Lists.newArrayList();

        // 积极的同步策略
        List<Map<String, Object>> positiveIncrement = Lists.newArrayList();

        // 保守的同步策略
        List<Map<String, Object>> negtiveIncrement = Lists.newArrayList();

        // 比较字段
        List<String> compareColumns = context.getColumns();


        // 首先获取到所有的moiDataId
        List<String> guids = allRecs.stream().map(x -> x.get(COMPARE_ID).toString()).collect(Collectors.toList());
        List<Map<String, Object>> existing = neo4jOperatorService.queryDataByGuid(driver, guids);

        Map<String, Map<String, Object>> existsData = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(existing)) {
            existsData = existing.stream().collect(Collectors.toMap(x -> x.get(COMPARE_ID).toString(), y -> y, (l, m) -> l));
        }

        *//** 已经存在的数据 *//*
        for (Map<String, Object> allRec : allRecs) {
            String compareId = allRec.get(COMPARE_ID).toString();
            Map<String, Object> esData = existsData.get(compareId);
            if (esData == null) {
                directoryInsert.add(allRec);
            }else {
                // 进行比较
                boolean flag = compareDifferences(allRec,esData,compareColumns);
                if (!flag) {
                    if (IncrementModeEnum.POSITIVE.getCode().equals(Integer.parseInt(context.getIncrementMode()))) {
                        // 积极: 先删除,后新增
                        positiveIncrement.add(allRec);
                    }else {
                        // 直接在原来的节点上进行修改
                        negtiveIncrement.add(allRec);
                    }
                }
            }
        }

        log.info("正在同步中....本次同步{}数量为{}个,同步模式为:{},当前已同步数量为{},同步节点结果:同步{}: 修改{}个,新增{}个",context.getNodeName(),allRecs.size(),IncrementModeEnum.POSITIVE.getCode().equals(Integer.parseInt(context.getIncrementMode()))?"积极":"保守", context.getNodeTotalCount(), context.getNodeName(),positiveIncrement.size() + negtiveIncrement.size(),
                directoryInsert.size());

        if (positiveIncrement.size() > 0) {
            this.generatorDataId(driver,context,positiveIncrement);
            neo4jOperatorService.positiveSync(driver,context,positiveIncrement);
        }
        if (negtiveIncrement.size() > 0) {
            neo4jOperatorService.negtiveSync(driver,context,negtiveIncrement);
        }

        // 新数据重新插入
        this.generatorDataId(driver,context,directoryInsert);
        for (Map<String, Object> stringObjectMap : directoryInsert) {
            stringObjectMap.put("collectType", CollectTypeEnum.ADD.getCode());
        }
        neo4jOperatorService.batchInsert(driver,context,directoryInsert);*/
    }


    /**
     * 返回true表示两个对象是相同的
     *
     * @param allRec
     * @param esData
     * @param compareColumns
     * @return
     */
    private boolean compareDifferences(Map<String, Object> allRec, Map<String, Object> esData, List<String> compareColumns) {
        // 如果比较字段存在指标字段，那么比较的时候不必比较指标字段，比较其他字段，如果其他字段相同，则创建和修改时间和原来保持一致，如果指标不同，则创建和修改时间取最新的，然后必定返回false
        final List<String> dataIdLists = Lists.newArrayList("moiParentDataId");
        List<String> temp = Lists.newArrayList(compareColumns);
        temp.addAll(dataIdLists);

        List<String> retainIndexField = temp.stream().filter(x -> indexField.contains(x)).collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(retainIndexField)) {
            // 存在指标字段
            temp.removeAll(retainIndexField);
            for (String compareColumn : temp) {
                if (dateFieldSet.contains(compareColumn)) continue;
                if (!Objects.equals(esData.get(compareColumn), allRec.get(compareColumn))) {
                    return false;
                }
            }
            // 设置旧的时间
            for (String ignoreFieldSet : dateFieldSet) {
                allRec.put(ignoreFieldSet, esData.get(ignoreFieldSet));
            }
            return false;
        } else {
            // 不存在指标字段
            for (String compareColumn : temp) {
                if (dateFieldSet.contains(compareColumn)) continue;
                if (!Objects.equals(esData.get(compareColumn), allRec.get(compareColumn))) {
                    return false;
                }
            }
            return true;
        }
    }
}
