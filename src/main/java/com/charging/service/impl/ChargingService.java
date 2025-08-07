package com.charging.service.impl;


import com.charging.service.TccAction;

import java.sql.*;
import java.util.*;

/**
 * 充电服务 - 实现TCC接口
 */
public class ChargingService implements TccAction {

    @Override
    public boolean tryAction(Connection conn, String txId, Object... args) throws SQLException {
        String chargingPointId = (String) args[0];
        String userId = (String) args[1];
        double amount = (double) args[2];
        
        // 幂等性检查
        if (isOperationProcessed(conn, txId, "TRY")) {
            System.out.println("充电Try操作已处理，幂等返回成功: " + txId);
            return true;
        }

        conn.setAutoCommit(false);
        boolean result = false;
        
        try {
            // 创建充电订单
            String orderId = UUID.randomUUID().toString();
            String insertSql = "INSERT INTO charging_order (order_id, tx_id, charging_point_id, user_id, amount, status, create_time) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setString(1, orderId);
                pstmt.setString(2, txId);
                pstmt.setString(3, chargingPointId);
                pstmt.setString(4, userId);
                pstmt.setDouble(5, amount);
                pstmt.setString(6, "INIT");
                pstmt.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
                pstmt.executeUpdate();
            }

            // 模拟充电尝试 - 随机失败以测试回滚逻辑
            boolean success = Math.random() > 0.3; // 70%成功率
            String status = success ? "IN_PROGRESS" : "FAILED";
            String remark = success ? "充电开始" : "充电尝试失败";

            // 更新订单状态
            String updateSql = "UPDATE charging_order SET status = ?, start_time = ? WHERE tx_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setString(1, status);
                pstmt.setTimestamp(2, success ? new Timestamp(System.currentTimeMillis()) : null);
                pstmt.setString(3, txId);
                pstmt.executeUpdate();
            }

            // 记录日志
            logChargingAction(conn, txId, orderId, chargingPointId, userId, amount, "TRY", 
                             success ? "SUCCESS" : "FAILED", remark);
            
            conn.commit();
            result = success;
            System.out.println(remark + ": " + orderId);
        } catch (SQLException e) {
            conn.rollback();
            logChargingAction(conn, txId, null, chargingPointId, userId, amount, "TRY", "FAILED", e.getMessage());
            System.err.println("充电Try操作失败: " + e.getMessage());
        }
        
        return result;
    }

    @Override
    public boolean confirmAction(Connection conn, String txId) throws SQLException {
        // 幂等性检查
        if (isOperationProcessed(conn, txId, "CONFIRM")) {
            System.out.println("充电Confirm操作已处理，幂等返回成功: " + txId);
            return true;
        }

        conn.setAutoCommit(false);
        boolean result = false;
        
        try {
            // 查询充电订单
            String selectSql = "SELECT * FROM charging_order WHERE tx_id = ? AND status = 'IN_PROGRESS'";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
                pstmt.setString(1, txId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("未找到有效的充电订单");
                    }
                    
                    String orderId = rs.getString("order_id");
                    String chargingPointId = rs.getString("charging_point_id");
                    String userId = rs.getString("user_id");
                    double amount = rs.getDouble("amount");
                    
                    // 更新订单状态为COMPLETED
                    String updateSql = "UPDATE charging_order SET status = 'COMPLETED', end_time = ? WHERE tx_id = ?";
                    try (PreparedStatement updatePstmt = conn.prepareStatement(updateSql)) {
                        updatePstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                        updatePstmt.setString(2, txId);
                        int rowsAffected = updatePstmt.executeUpdate();
                        if (rowsAffected != 1) {
                            throw new SQLException("更新充电订单状态失败");
                        }
                    }
                    
                    // 记录日志
                    logChargingAction(conn, txId, orderId, chargingPointId, userId, amount, 
                                     "CONFIRM", "SUCCESS", "充电完成");
                    
                    conn.commit();
                    result = true;
                    System.out.println("确认充电成功: " + orderId);
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            logChargingAction(conn, txId, null, null, null, 0, "CONFIRM", "FAILED", e.getMessage());
            System.err.println("充电Confirm操作失败: " + e.getMessage());
            throw e;
        }
        
        return result;
    }

    @Override
    public boolean cancelAction(Connection conn, String txId) throws SQLException {
        // 幂等性检查
        if (isOperationProcessed(conn, txId, "CANCEL")) {
            System.out.println("充电Cancel操作已处理，幂等返回成功: " + txId);
            return true;
        }

        conn.setAutoCommit(false);
        boolean result = false;
        
        try {
            // 查询充电订单
            String selectSql = "SELECT * FROM charging_order WHERE tx_id = ? AND status IN ('INIT', 'IN_PROGRESS')";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
                pstmt.setString(1, txId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        // 没有找到可取消的订单，可能已经处理过
                        logChargingAction(conn, txId, null, null, null, 0, "CANCEL", "SUCCESS", "无订单需要取消");
                        conn.commit();
                        return true;
                    }
                    
                    String orderId = rs.getString("order_id");
                    String chargingPointId = rs.getString("charging_point_id");
                    String userId = rs.getString("user_id");
                    double amount = rs.getDouble("amount");
                    
                    // 更新订单状态为CANCELED
                    String updateSql = "UPDATE charging_order SET status = 'CANCELED', cancel_time = ? WHERE tx_id = ?";
                    try (PreparedStatement updatePstmt = conn.prepareStatement(updateSql)) {
                        updatePstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                        updatePstmt.setString(2, txId);
                        int rowsAffected = updatePstmt.executeUpdate();
                        if (rowsAffected != 1) {
                            throw new SQLException("更新充电订单状态失败");
                        }
                    }
                    
                    // 记录日志
                    logChargingAction(conn, txId, orderId, chargingPointId, userId, amount, 
                                     "CANCEL", "SUCCESS", "充电取消");
                    
                    conn.commit();
                    result = true;
                    System.out.println("充电已取消: " + orderId);
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            logChargingAction(conn, txId, null, null, null, 0, "CANCEL", "FAILED", e.getMessage());
            System.err.println("充电Cancel操作失败: " + e.getMessage());
            throw e;
        }
        
        return result;
    }

    // 记录充电操作日志
    private void logChargingAction(Connection conn, String txId, String orderId, String chargingPointId, 
                                  String userId, double amount, String action, String status, String remark) throws SQLException {
        String sql = "INSERT INTO charging_log (tx_id, order_id, charging_point_id, user_id, amount, action, status, remark, create_time) " +
                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            pstmt.setString(2, orderId);
            pstmt.setString(3, chargingPointId);
            pstmt.setString(4, userId);
            pstmt.setDouble(5, amount);
            pstmt.setString(6, action);
            pstmt.setString(7, status);
            pstmt.setString(8, remark);
            pstmt.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
            pstmt.executeUpdate();
        }
    }

    // 检查操作是否已经处理过（幂等性检查）
    private boolean isOperationProcessed(Connection conn, String txId, String action) throws SQLException {
        String sql = "SELECT COUNT(*) FROM charging_log WHERE tx_id = ? AND action = ? AND status = 'SUCCESS'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            pstmt.setString(2, action);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    public Map<String, Object> getOrder(Connection conn, String txId) throws SQLException {
        String sql = "SELECT * FROM charging_order WHERE tx_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> order = new HashMap<>();
                    order.put("order_id", rs.getString("order_id"));
                    order.put("tx_id", rs.getString("tx_id"));
                    order.put("charging_point_id", rs.getString("charging_point_id"));
                    order.put("user_id", rs.getString("user_id"));
                    order.put("amount", rs.getDouble("amount"));
                    order.put("status", rs.getString("status"));
                    return order;
                }
            }
        }
        return null;
    }
    
    // 获取异常充电订单
    public List<String> getAbnormalChargingTxIds(Connection conn, long timeoutMinutes) throws SQLException {
        List<String> txIds = new ArrayList<>();
        Timestamp cutoffTime = new Timestamp(System.currentTimeMillis() - timeoutMinutes * 60 * 1000);
        
        // 查询长时间处于INIT或IN_PROGRESS状态的订单
        String sql = "SELECT tx_id FROM charging_order " +
                   "WHERE status IN ('INIT', 'IN_PROGRESS') " +
                   "AND create_time < ? " +
                   "AND tx_id NOT IN (SELECT tx_id FROM charging_log WHERE action IN ('CONFIRM', 'CANCEL') AND status = 'SUCCESS')";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, cutoffTime);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    txIds.add(rs.getString("tx_id"));
                }
            }
        }
        return txIds;
    }
}
    