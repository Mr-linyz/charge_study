package com.charging.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 数据库配置类
 */
public class DBConfig {
    private static final Properties properties = new Properties();
    
    static {
        try (InputStream input = DBConfig.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                throw new RuntimeException("无法找到db.properties配置文件");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("加载数据库配置失败", ex);
        }
    }
    
    public static String getUrl() {
        return properties.getProperty("db.url");
    }
    
    public static String getUsername() {
        return properties.getProperty("db.username");
    }
    
    public static String getPassword() {
        return properties.getProperty("db.password");
    }
    
    public static String getDriverClass() {
        return properties.getProperty("db.driver-class");
    }
}
    