package cn.com.bluemoon.metadata.base.util;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

/**
 * 
 * @Date 2021/1/6 10:28
 * @Version 1.0
 */
public class JavaDriverFactory {

    private static Driver driver = null;

    public static void init(String uri, String userName, String passwd) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(userName, passwd));
    }

    public static void close() {
        if (driver != null) {
            driver.close();
        }
    }

    public static Driver getDriver() {
        return driver;
    }
}
