package org.example.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据库工具类，管理数据库连接和表初始化
 */
public class DBUtil {
    private static final String URL = "jdbc:mysql://localhost:3306/charging_db?useSSL=false&serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    
    // 静态代码块，在类加载时执行驱动加载
    static {
        try {
            // 加载MySQL驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("MySQL JDBC驱动加载成功");
        } catch (ClassNotFoundException e) {
            System.err.println("无法加载MySQL JDBC驱动，请确保驱动已添加到类路径中");
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    public static void initTables() {
        try (Connection conn = getConnection()) {
            // 新增积分相关表
            // 用户积分表
            String createUserPointsTable = "CREATE TABLE IF NOT EXISTS user_points (" +
                                         "user_id VARCHAR(50) PRIMARY KEY," +
                                         "total_points INT NOT NULL DEFAULT 0," +
                                         "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                                         "update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                                         "FOREIGN KEY (user_id) REFERENCES user_account(user_id)" +
                                         ")";
            
            // 积分交易记录表
            String createPointsTransactionTable = "CREATE TABLE IF NOT EXISTS points_transaction (" +
                                                "transaction_id VARCHAR(50) PRIMARY KEY," +
                                                "user_id VARCHAR(50) NOT NULL," +
                                                "order_id VARCHAR(50) NOT NULL," +
                                                "points INT NOT NULL," +
                                                "type VARCHAR(50) NOT NULL," +
                                                "status VARCHAR(20) NOT NULL," +
                                                "create_time TIMESTAMP NOT NULL," +
                                                "complete_time TIMESTAMP," +
                                                "remark VARCHAR(255)," +
                                                "FOREIGN KEY (user_id) REFERENCES user_account(user_id)," +
                                                "UNIQUE KEY unique_order_id (order_id)," +
                                                "INDEX idx_user_id (user_id)" +
                                                ")";
            
            // 本地消息表
            String createLocalMessageTable = "CREATE TABLE IF NOT EXISTS local_message (" +
                                           "message_id VARCHAR(50) PRIMARY KEY," +
                                           "business_type VARCHAR(50) NOT NULL," +
                                           "business_id VARCHAR(50) NOT NULL," +
                                           "message_content TEXT NOT NULL," +
                                           "status VARCHAR(20) NOT NULL," +
                                           "retry_count INT NOT NULL DEFAULT 0," +
                                           "next_retry_time TIMESTAMP NOT NULL," +
                                           "create_time TIMESTAMP NOT NULL," +
                                           "update_time TIMESTAMP NOT NULL," +
                                           "remark VARCHAR(255)," +
                                           "INDEX idx_status_retry (status, next_retry_time)," +
                                           "INDEX idx_business (business_type, business_id)" +
                                           ")";
            
            // 修改充电订单表，增加结算状态
            String alterChargingOrderTable = "ALTER TABLE charging_order " +
                                           "ADD COLUMN settlement_status VARCHAR(20) DEFAULT 'UNSETTLED'," +
                                           "ADD COLUMN settlement_time TIMESTAMP";
            
            try (Statement stmt = conn.createStatement()) {
                // 执行表结构创建
                stmt.execute(createUserPointsTable);
                stmt.execute(createPointsTransactionTable);
                stmt.execute(createLocalMessageTable);
                
                // 尝试修改表结构，如果已经修改过会抛出异常，这里忽略
                try {
                    stmt.execute(alterChargingOrderTable);
                } catch (SQLException e) {
                    // 表结构可能已经存在，忽略此异常
                    System.out.println("充电订单表可能已包含结算状态字段，忽略修改");
                }
                
                System.out.println("积分相关表结构初始化完成");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
    