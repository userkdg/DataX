package cn.com.bluemoon.metadata.base.util;

import cn.com.bluemoon.metadata.base.constant.Neo4jCypherConstants;

import java.util.List;

/**
 * 
 * @Date 2021/1/6 15:07
 * @Version 1.0
 */
public class CypherHelper {

    public static String generatePropertyMap(List<String> columns, String n, String batch) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sb.append(n).append('.').append(columns.get(i)).append("=").append(batch).append('.').append(columns.get(i));
            if (i != columns.size()-1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String s = generatePropertyMap(Neo4jCypherConstants.BASE_RELATION_COLUMNS, "rel", "batch");
        System.out.println(s);
    }

}
