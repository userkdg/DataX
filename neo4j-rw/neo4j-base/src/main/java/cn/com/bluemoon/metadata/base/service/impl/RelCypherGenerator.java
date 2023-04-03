package cn.com.bluemoon.metadata.base.service.impl;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;
import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;
import cn.com.bluemoon.metadata.base.service.ICypherGenerator;
import cn.com.bluemoon.metadata.base.util.CypherHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 关系语句生成
 * 
 * @Date 2021/1/7 15:26
 * @Version 1.0
 */
public class RelCypherGenerator implements ICypherGenerator {

    private Logger logger = LoggerFactory.getLogger(RelCypherGenerator.class);

    @Override
    public String generateCreateCql(CreateTypeConfig context) {
        final String VAR_REL = "rel";
        String srcLabel = context.getSrcLabel();
        String srcDegreeDir = context.getSrcDegreeDir();
        String targetLabel = context.getTargetLabel();
        String targetDegreeDir = context.getTargetDegreeDir();
        String relLabel = context.getRelLabel();
        String mode = context.getMode();
        String srcPattern = "moiDataId:#sourceDataId,moiIsNewestVersion:1";
        String targetPattern = "moiDataId:#targetDataId,moiIsNewestVersion:1";


        //UNWIND  $batches AS batch MATCH (p:UserPhone{phone_num:batch.phone_num}), (h:HH{hh:batch.hh}) MERGE (p)-[belongs:Belongs_test3]->(h)
        String header = "UNWIND  $batches AS batch ";
        String body = " MATCH (s:`" + srcLabel + "`";
        if (StringUtils.isNotEmpty(srcPattern)) {
            srcPattern = srcPattern.replaceAll("#", "batch.");
            srcPattern = "{" + srcPattern + "}";
            body += srcPattern;
        }
        body += ") , (t:`" + targetLabel + "`";
        if (StringUtils.isNotEmpty(targetPattern)) {
            targetPattern = targetPattern.replaceAll("#", "batch.");
            targetPattern = "{" + targetPattern + "}";
            body += targetPattern;
        }
        body += ")";
        body = body +" " +mode + "(s)" + srcDegreeDir + "[" + VAR_REL + ":`" + relLabel + "`]" + targetDegreeDir + "(t)";

        if (CollectionUtils.isNotEmpty(Neo4jCypherConstants.BASE_RELATION_COLUMNS)) {
            String mergeProperties = CypherHelper.generatePropertyMap(Neo4jCypherConstants.BASE_RELATION_COLUMNS,"rel","batch");
//        ON CREATE  SET belongs.content= batch.content,belongs.call_time=batch.call_time
//        ON MATCH  SET belongs.content= batch.content,belongs.call_time=batch.call_time
            body = body + " ON CREATE  SET " + mergeProperties;
        }
        String cypherQl = header + body;

        logger.info("Generated cypher ql:{}", cypherQl);
        return cypherQl;
    }

    @Override
    public String generateUpdateCql(CreateTypeConfig context) {
        return null;
    }
}
