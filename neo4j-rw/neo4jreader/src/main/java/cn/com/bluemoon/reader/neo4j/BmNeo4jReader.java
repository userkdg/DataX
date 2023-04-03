package cn.com.bluemoon.reader.neo4j;

import cn.com.bluemoon.metadata.base.util.JavaDriverFactory;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Lilike on 2022/4/21
 */
public class BmNeo4jReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void init() {
            log.info("开始BmNeo4jReaderJob的初始化！");
            this.conf = super.getPluginJobConf();//获取配置文件信息{parameter 里面的参数}
            Map<String, String> params = conf.getMap("", String.class);
            try {
                BmNeo4jReaderParamHelper.validParameters(params);
            } catch (Exception e) {
                log.error("Neo4jWriter params:{}", conf.toJSON());
                throw new RuntimeException(e);
            }
        }

        @Override
        public void prepare() {
            // todo 开始
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return SimpleBmNeo4jReaderSplitUtils.doSplit(this.getPluginJobConf(), adviceNumber);
        }

        @Override
        public void post() {
            // todo
        }

        @Override
        public void destroy() {
            // todo
        }

    }

    public static class Task extends Reader.Task {

        private Configuration rawConf;
        private Map<String, Object> taskConfig;

        private String dbUserName;
        private String dbPassword;
        private String queryCql;
        private String neo4jUrl;

        @Override
        public void init() {
            this.rawConf = super.getPluginJobConf();
            this.taskConfig = rawConf.getMap("", Object.class);
            this.dbUserName = taskConfig.get("username").toString();
            this.dbPassword = taskConfig.get("password").toString();
            this.neo4jUrl = taskConfig.get("neo4jUrl").toString();
            this.queryCql = taskConfig.get("queryCql").toString();
            JavaDriverFactory.init(neo4jUrl, dbUserName, dbPassword);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            // 读数据
            Neo4jWriterHelper.read(recordSender, super.getTaskPluginCollector(),this.queryCql);

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            JavaDriverFactory.close();
        }
    }
}



