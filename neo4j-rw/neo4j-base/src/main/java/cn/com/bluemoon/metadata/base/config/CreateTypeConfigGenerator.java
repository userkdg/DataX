package cn.com.bluemoon.metadata.base.config;

import com.alibaba.fastjson2.JSONArray;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants.CREATE_TYPE_ON_NODE;
import static cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants.CREATE_TYPE_ON_REL;

/**
 * 
 * @Date 2021/1/6 10:27
 * @Version 1.0
 */
public class CreateTypeConfigGenerator {

    /**
     * 生成配置信息
     *
     * @param taskConfig
     * @return
     */
    public static CreateTypeConfig generate(Map<String, Object> taskConfig) {
        CreateTypeConfig config = new CreateTypeConfig();
        JSONArray column = (JSONArray) taskConfig.get("column");
        List<String> columns = new ArrayList<>();
        for (Object o : column) {
            columns.add(o.toString());
        }
        config.setColumns(columns);
        config.setCreateType(taskConfig.get("create.type").toString());
        config.setEnvCn(taskConfig.get("create.envCn").toString());
        config.setCatalogCN(taskConfig.get("create.catalogCN").toString());
        if (config.getCreateType().equals(CREATE_TYPE_ON_NODE)) {
            config.setMode(taskConfig.get("create.NODE.mode").toString());
            config.setLabel(taskConfig.get("create.NODE.label").toString());
            config.setNodeName(taskConfig.get("create.NODE.nodeName").toString());
            config.setCompareId(taskConfig.get("create.NODE.compare.id").toString());
            String collectTime = taskConfig.get("create.NODE.collectTime").toString();
            if (StringUtils.isNotEmpty(collectTime)) {
                config.setCollectTime(Long.parseLong(collectTime));
            }
            config.setIncrementMode(taskConfig.get("create.NODE.incrementMode").toString());
        } else if (config.getCreateType().equals(CREATE_TYPE_ON_REL)) {
            config.setSrcLabel(taskConfig.get("create.RELATIONSHIP.src.label").toString());
            config.setTargetLabel(taskConfig.get("create.RELATIONSHIP.target.label").toString());
            config.setRelationType(taskConfig.get("create.RELATIONSHIP.relationType").toString());
            config.setRelLabel(taskConfig.get("create.RELATIONSHIP.label").toString());
            config.setMode(taskConfig.get("create.RELATIONSHIP.mode").toString());
            config.setSrcDegreeDir(taskConfig.get("create.RELATIONSHIP.src.degree.dir").toString());
            config.setTargetDegreeDir(taskConfig.get("create.RELATIONSHIP.target.degree.dir").toString());
        }

        return config;
    }
}
