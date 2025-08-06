package org.example.service.impl;


import org.example.service.TccAction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 支付服务 - 实现TCC接口
 */
public class PaymentService implements TccAction {

    @Override
    public boolean tryAction(Connection conn, String txId, Object... args) throws SQLException {
        String userId = (String) args[0];
        double amount = (double) args[1];
        
        // 幂等性检查 - 如果已经处理过，直接返回成功
        if (isOperationProcessed(conn, txId, "TRY")) {
            System.out.println("支付Try操作已处理，幂等返回成功: " + txId);
            return true;
        }

        conn.setAutoCommit(false);
        boolean result = false;
        
        try {
            // 检查余额
            String checkSql = "SELECT balance FROM user_account WHERE user_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(checkSql)) {
                pstmt.setString(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next() || rs.getDouble("balance") < amount) {
                        System.out.println("余额不足或用户不存在，支付失败");
                        logPaymentAction(conn, txId, userId, amount, "TRY", "FAILED", "余额不足或用户不存在");
                        conn.commit();
                        return false;
                    }
                }
            }

            // 扣减余额
            String updateSql = "UPDATE user_account SET balance = balance - ? WHERE user_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setDouble(1, amount);
                pstmt.setString(2, userId);
                int rowsAffected = pstmt.executeUpdate();
                if (rowsAffected != 1) {
                    throw new SQLException("扣减余额失败");
                }
            }

            // 记录预扣信息
            String insertHoldSql = "INSERT INTO payment_pre_hold (tx_id, user_id, amount, status) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertHoldSql)) {
                pstmt.setString(1, txId);
                pstmt.setString(2, userId);
                pstmt.setDouble(3, amount);
                pstmt.setString(4, "HOLD");
                pstmt.executeUpdate();
            }

            // 记录日志
            logPaymentAction(conn, txId, userId, amount, "TRY", "SUCCESS", null);
            
            conn.commit();
            result = true;
            System.out.println("冻结金额: " + amount + " 来自用户: " + userId);
        } catch (SQLException e) {
            conn.rollback();
            logPaymentAction(conn, txId, userId, amount, "TRY", "FAILED", e.getMessage());
            System.err.println("支付Try操作失败: " + e.getMessage());
        }
        
        return result;
    }

    @Override
    public boolean confirmAction(Connection conn, String txId) throws SQLException {
        // 幂等性检查
        if (isOperationProcessed(conn, txId, "CONFIRM")) {
            System.out.println("支付Confirm操作已处理，幂等返回成功: " + txId);
            return true;
        }

        conn.setAutoCommit(false);
        boolean result = false;
        
        try {
            // 查询预扣记录
            String selectSql = "SELECT * FROM payment_pre_hold WHERE tx_id = ? AND status = 'HOLD'";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
                pstmt.setString(1, txId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("未找到有效的预扣记录");
                    }
                    
                    String userId = rs.getString("user_id");
                    double amount = rs.getDouble("amount");
                    
                    // 更新预扣记录状态为CONFIRMED
                    String updateSql = "UPDATE payment_pre_hold SET status = 'CONFIRMED' WHERE tx_id = ?";
                    try (PreparedStatement updatePstmt = conn.prepareStatement(updateSql)) {
                        updatePstmt.setString(1, txId);
                        int rowsAffected = updatePstmt.executeUpdate();
                        if (rowsAffected != 1) {
                            throw new SQLException("更新预扣记录失败");
                        }
                    }
                    
                    // 记录日志
                    logPaymentAction(conn, txId, userId, amount, "CONFIRM", "SUCCESS", null);
                    conn.commit();
                    result = true;
                    System.out.println("确认支付成功: " + txId);
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            logPaymentAction(conn, txId, null, 0, "CONFIRM", "FAILED", e.getMessage());
            System.err.println("支付Confirm操作失败: " + e.getMessage());
            throw e;
        }
        
        return result;
    }

    @Override
    public boolean cancelAction(Connection conn, String txId) throws SQLException {
        // 幂等性检查
        if (isOperationProcessed(conn, txId, "CANCEL")) {
            System.out.println("支付Cancel操作已处理，幂等返回成功: " + txId);
            return true;
        }

        conn.setAutoCommit(false);
        boolean result = false;
        
        try {
            // 查询预扣记录
            String selectSql = "SELECT * FROM payment_pre_hold WHERE tx_id = ? AND status = 'HOLD'";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
                pstmt.setString(1, txId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        // 没有找到预扣记录，可能已经处理过
                        logPaymentAction(conn, txId, null, 0, "CANCEL", "SUCCESS", "无预扣记录需要取消");
                        conn.commit();
                        return true;
                    }
                    
                    String userId = rs.getString("user_id");
                    double amount = rs.getDouble("amount");
                    
                    // 退还金额
                    String refundSql = "UPDATE user_account SET balance = balance + ? WHERE user_id = ?";
                    try (PreparedStatement refundPstmt = conn.prepareStatement(refundSql)) {
                        refundPstmt.setDouble(1, amount);
                        refundPstmt.setString(2, userId);
                        int rowsAffected = refundPstmt.executeUpdate();
                        if (rowsAffected != 1) {
                            throw new SQLException("退还金额失败");
                        }
                    }

                    // 更新预扣记录状态为CANCELED
                    String updateSql = "UPDATE payment_pre_hold SET status = 'CANCELED' WHERE tx_id = ?";
                    try (PreparedStatement updatePstmt = conn.prepareStatement(updateSql)) {
                        updatePstmt.setString(1, txId);
                        int rowsAffected = updatePstmt.executeUpdate();
                        if (rowsAffected != 1) {
                            throw new SQLException("更新预扣记录状态失败");
                        }
                    }
                    
                    // 记录日志
                    logPaymentAction(conn, txId, userId, amount, "CANCEL", "SUCCESS", null);
                    conn.commit();
                    result = true;
                    System.out.println("支付已取消，金额已退还: " + txId);
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            logPaymentAction(conn, txId, null, 0, "CANCEL", "FAILED", e.getMessage());
            System.err.println("支付Cancel操作失败: " + e.getMessage());
            throw e;
        }
        
        return result;
    }

    // 记录支付操作日志
    private void logPaymentAction(Connection conn, String txId, String userId, double amount, 
                                 String action, String status, String remark) throws SQLException {
        String sql = "INSERT INTO payment_log (tx_id, user_id, amount, action, status, remark, create_time) " +
                   "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            pstmt.setString(2, userId);
            pstmt.setDouble(3, amount);
            pstmt.setString(4, action);
            pstmt.setString(5, status);
            pstmt.setString(6, remark);
            pstmt.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
            pstmt.executeUpdate();
        }
    }

    // 检查操作是否已经处理过（幂等性检查）
    private boolean isOperationProcessed(Connection conn, String txId, String action) throws SQLException {
        String sql = "SELECT COUNT(*) FROM payment_log WHERE tx_id = ? AND action = ? AND status = 'SUCCESS'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            pstmt.setString(2, action);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    public double getUserBalance(Connection conn, String userId) throws SQLException {
        String sql = "SELECT balance FROM user_account WHERE user_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getDouble("balance");
                }
            }
        }
        return 0.0;
    }
    
    // 获取异常支付订单
    public List<String> getAbnormalPaymentTxIds(Connection conn, long timeoutMinutes) throws SQLException {
        List<String> txIds = new ArrayList<>();
        Timestamp cutoffTime = new Timestamp(System.currentTimeMillis() - timeoutMinutes * 60 * 1000);
        
        // 查询处于TRY_SUCCESS但未确认或取消的支付记录
        String sql = "SELECT DISTINCT tx_id FROM payment_log " +
                   "WHERE status = 'SUCCESS' AND action = 'TRY' " +
                   "AND create_time < ? " +
                   "AND tx_id NOT IN (SELECT tx_id FROM payment_log WHERE action IN ('CONFIRM', 'CANCEL') AND status = 'SUCCESS')";
        
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
    