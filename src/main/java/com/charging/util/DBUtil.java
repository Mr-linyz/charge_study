package com.charging.util;

import com.charging.config.DBConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库工具类
 */
@Slf4j
public class DBUtil {
    private static volatile boolean driverLoaded = false;

    /**
     * 加载数据库驱动
     */
    private static void loadDriver() {
        if (!driverLoaded) {
            synchronized (DBUtil.class) {
                if (!driverLoaded) {
                    try {
                        Class.forName(DBConfig.getDriverClass());
                        driverLoaded = true;
                        log.info("数据库驱动加载成功: {}", DBConfig.getDriverClass());
                    } catch (ClassNotFoundException e) {
                        log.error("数据库驱动加载失败", e);
                        throw new RuntimeException("数据库驱动加载失败", e);
                    }
                }
            }
        }
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection() throws SQLException {
        loadDriver();
        return DriverManager.getConnection(
            DBConfig.getUrl(),
            DBConfig.getUsername(),
            DBConfig.getPassword()
        );
    }

    /**
     * 关闭资源
     */
    public static void close(Connection conn, PreparedStatement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.error("关闭ResultSet失败", e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error("关闭PreparedStatement失败", e);
            }
        }
        if (conn != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error("关闭Connection失败", e);
            }
        }
    }

    /**
     * 执行更新操作
     */
    public static int executeUpdate(Connection conn, String sql, Object... params) throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, params);
            return stmt.executeUpdate();
        } finally {
            close(null, stmt, null);
        }
    }

    /**
     * 执行查询操作
     */
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Object... params) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, params);
            rs = stmt.executeQuery();

            List<Map<String, Object>> result = new ArrayList<>();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                result.add(row);
            }
            return result;
        } finally {
            close(null, stmt, rs);
        }
    }

    /**
     * 设置SQL参数
     */
    private static void setParameters(PreparedStatement stmt, Object... params) throws SQLException {
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
        }
    }

    /**
     * 初始化数据库表结构
     */
    public static void initTables() {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(true);

            // 创建用户积分表
            executeUpdate(conn, "CREATE TABLE IF NOT EXISTS user_points (" +
                "user_id VARCHAR(50) PRIMARY KEY," +
                "total_points DECIMAL(10,2) NOT NULL DEFAULT 0," +
                "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");

            // 创建积分交易明细表
            executeUpdate(conn, "CREATE TABLE IF NOT EXISTS points_transaction (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "user_id VARCHAR(50) NOT NULL," +
                "order_id VARCHAR(50) NOT NULL UNIQUE," +
                "points DECIMAL(10,2) NOT NULL," +
                "transaction_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");

            // 创建本地消息表
            executeUpdate(conn, "CREATE TABLE IF NOT EXISTS local_message (" +
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
            executeUpdate(conn, "CREATE TABLE IF NOT EXISTS failed_points_message (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "message_id VARCHAR(50) NOT NULL," +
                "order_id VARCHAR(50) NOT NULL," +
                "user_id VARCHAR(50) NOT NULL," +
                "points DECIMAL(10,2) NOT NULL," +
                "error_message TEXT," +
                "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");

            log.info("数据库表结构初始化完成");
        } catch (SQLException e) {
            log.error("数据库表结构初始化失败", e);
        } finally {
            close(conn, null, null);
        }
    }
}
    