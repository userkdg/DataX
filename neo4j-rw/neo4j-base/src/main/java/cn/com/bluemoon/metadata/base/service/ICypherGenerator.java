package cn.com.bluemoon.metadata.base.service;

import cn.com.bluemoon.metadata.base.config.CreateTypeConfig;

/**
 * 
 * @Date 2021/1/6 14:44
 * @Version 1.0
 */
public interface ICypherGenerator {

    /**
     * 生成Create的Cql
     * @param context
     * @return
     */
    String generateCreateCql(CreateTypeConfig context);

    /**
     * 生成Update的Cql
     * @param context
     * @return
     */
    String generateUpdateCql(CreateTypeConfig context);

}
