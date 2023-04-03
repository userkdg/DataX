package cn.com.bluemoon.reader.neo4j;

import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lilike on 2022/4/22
 */
public class SimpleBmNeo4jReaderSplitUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleBmNeo4jReaderSplitUtils.class);

    /**
     * 目前先根据SQL的数量来分配分片数量
     * @param originalSliceConfig
     * @param adviceNumber
     * @return
     */
    public static List<Configuration> doSplit(Configuration originalSliceConfig, int adviceNumber) {
        LOG.info("开始切分任务");
        // 获取到CQL
        List<Object> conns = originalSliceConfig.getList("connection", Object.class);
        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        Configuration tempSlice;
        for (int i = 0, len = conns.size(); i < len; i++) {

            Configuration sliceConfig = originalSliceConfig.clone();
            Configuration connConf = Configuration.from(conns.get(i).toString());
            sliceConfig.remove("connection");

            // 说明是配置的 querySql 方式
            List<String> sqls = connConf.getList("queryCql", String.class);
            String neo4jUrl = connConf.getList("neo4jUrl", String.class).get(0);
            for (String querySql : sqls) {
                tempSlice = sliceConfig.clone();
                tempSlice.set("queryCql", querySql);
                tempSlice.set("neo4jUrl",neo4jUrl);
                splittedConfigs.add(tempSlice);
            }
        }
        return splittedConfigs;
    }


}
