package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.ICypherGenerator;
import cn.com.bluemoon.metadata.base.util.CypherHelper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @Date 2021/1/6 14:48
 * @Version 1.0
 */
@SuppressWarnings("Duplicates")
public class NodeCypherGenerator implements ICypherGenerator {

    private Logger logger = LoggerFactory.getLogger(NodeCypherGenerator.class);

    @Override
    public String generateCreateCql(CreateTypeConfig context) {
        String label = context.getLabel();
        List<String> columns = context.getColumns();
        List<String> baseColumns = Lists.newArrayList(Neo4jCypherConstants.BASE_NODE_COLUMNS);
        baseColumns.removeAll(columns);
        List<String> tempColumns = new ArrayList<>(columns);
        tempColumns.addAll(baseColumns);
        String propStr = CypherHelper.generatePropertyMap(tempColumns,"n","batch");

        //UNWIND  $batches AS batch CREATE (h:HH_flume1) set h.id=batch.c0,h.hh=batch.c1, h.address=batch.c2
        String header = "UNWIND  $batches AS batch CREATE " + " (n:`Neo4jNodeBaseEntity`:`" + label + "`";
        header += ")";
        String cypherQl = header + " set " + propStr;

        logger.info("Generated cypher ql:{}", cypherQl);
        return cypherQl;
    }

    @Override
    public String generateUpdateCql(CreateTypeConfig context) {
        List<String> columns = context.getColumns();
        //List<String> baseColumns = Lists.newArrayList(Neo4jCypherConstants.BASE_NODE_COLUMNS);
        //baseColumns.removeAll(columns);
        //List<String> tempColumns = new ArrayList<>(columns);
        //tempColumns.addAll(baseColumns);
        String propStr = CypherHelper.generatePropertyMap(columns,"n","batch");
        String label = context.getLabel();

        String header = "UNWIND  $batches AS batch match " + " (n:`Neo4jNodeBaseEntity`:`" + label + "`)";
        header += " where n.guid=batch.guid and n.moiIsNewestVersion = 1";
        String cypherQl = header + " set " + propStr + " return n";

        logger.info("Generated cypher ql:{}", cypherQl);
        return cypherQl;
    }

}
