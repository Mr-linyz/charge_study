package com.charging.util;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 数据库工具类，管理数据库连接和初始化
 */
@Slf4j
public class DBUtil {
    private static String url;
    private static String username;
    private static String password;
    private static String driverClass;

    // 静态初始化块，加载配置
    static {
        try {
            Properties props = new Properties();
            props.load(DBUtil.class.getClassLoader().getResourceAsStream("db.properties"));

            url = props.getProperty("db.url");
            username = props.getProperty("db.username");
            password = props.getProperty("db.password");
            driverClass = props.getProperty("db.driver-class");

            // 加载数据库驱动
            Class.forName(driverClass);
        } catch (Exception e) {
            log.error("数据库配置初始化失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据库配置初始化失败", e);
        }
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * 初始化数据库表结构
     */
    public static void initTables() {
        // 创建充电订单表
        executeUpdate("CREATE TABLE IF NOT EXISTS charging_order (" +
            "id INT AUTO_INCREMENT PRIMARY KEY," +
            "order_id VARCHAR(50) NOT NULL UNIQUE," +
            "user_id VARCHAR(50) NOT NULL," +
            "amount DECIMAL(10,2) NOT NULL," +
            "status VARCHAR(20) NOT NULL COMMENT 'CHARGING, PENDING, SETTLED, CANCELLED'," +
            "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "settlement_time TIMESTAMP NULL)");

        // 创建用户积分表
        executeUpdate("CREATE TABLE IF NOT EXISTS user_points (" +
            "user_id VARCHAR(50) PRIMARY KEY," +
            "total_points DECIMAL(10,2) NOT NULL DEFAULT 0," +
            "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");

        // 创建积分交易明细表
        executeUpdate("CREATE TABLE IF NOT EXISTS points_transaction (" +
            "id INT AUTO_INCREMENT PRIMARY KEY," +
            "user_id VARCHAR(50) NOT NULL," +
            "order_id VARCHAR(50) NOT NULL UNIQUE," +
            "points DECIMAL(10,2) NOT NULL," +
            "transaction_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "FOREIGN KEY (user_id) REFERENCES user_points(user_id))");

        // 创建本地消息表
        executeUpdate("CREATE TABLE IF NOT EXISTS local_message (" +
            "id VARCHAR(50) PRIMARY KEY," +
            "order_id VARCHAR(50) NOT NULL UNIQUE," +
            "user_id VARCHAR(50) NOT NULL," +
            "points DECIMAL(10,2) NOT NULL," +
            "status VARCHAR(20) NOT NULL COMMENT 'PENDING, SENT, PROCESSED, FAILED'," +
            "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "send_time TIMESTAMP NULL," +
            "process_time TIMESTAMP NULL," +
            "retry_count INT NOT NULL DEFAULT 0)");

        // 创建失败消息记录表
        executeUpdate("CREATE TABLE IF NOT EXISTS failed_points_message (" +
            "id INT AUTO_INCREMENT PRIMARY KEY," +
            "message_id VARCHAR(50) NOT NULL," +
            "order_id VARCHAR(50) NOT NULL," +
            "user_id VARCHAR(50) NOT NULL," +
            "points DECIMAL(10,2) NOT NULL," +
            "error_message TEXT," +
            "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
    }

    /**
     * 执行更新语句
     */
    public static void executeUpdate(String sql) {
        try (Connection conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.executeUpdate();
            log.info("执行SQL成功: {}", sql.substring(0, Math.min(50, sql.length())) + "...");
        } catch (SQLException e) {
            log.error("执行SQL失败: {}", e.getMessage());
        }
    }

    /**
     * 关闭数据库连接（用于演示程序）
     */
    public static void close() {
        // 对于DriverManager管理的连接，不需要显式关闭连接池
        log.info("数据库连接已关闭");
    }
}
    