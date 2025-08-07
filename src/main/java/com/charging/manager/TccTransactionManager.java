package com.charging.manager;

import com.charging.service.TccAction;

import java.sql.*;

/**
 * TCC事务管理器
 */
public class TccTransactionManager {

    /**
     * 开始一个新事务
     */
    public String beginTransaction(Connection conn) throws SQLException {
        String txId = java.util.UUID.randomUUID().toString();

        conn.setAutoCommit(false);

        // 记录事务
        String sql = "INSERT INTO transaction_record (tx_id, status, create_time) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            pstmt.setString(2, "INIT");
            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            pstmt.executeUpdate();
        }

        conn.commit();

        System.out.println("开始事务: " + txId);
        return txId;
    }

    /**
     * 执行Try操作
     */
    public boolean executeTry(Connection conn, String txId, TccAction action, Object... args) throws SQLException {
        conn.setAutoCommit(false);

        try {
            // 执行Try操作
            boolean result = action.tryAction(conn, txId, args);

            // 更新事务状态
            String status = result ? "TRY_SUCCESS" : "TRY_FAILED";
            String updateSql = "UPDATE transaction_record SET status = ?, update_time = ? WHERE tx_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setString(1, status);
                pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                pstmt.setString(3, txId);
                pstmt.executeUpdate();
            }

            conn.commit();
            return result;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    /**
     * 提交事务
     */
    public boolean commit(Connection conn, String txId, TccAction... actions) throws SQLException {
        conn.setAutoCommit(false);

        try {
            // 检查事务状态
            String checkSql = "SELECT status FROM transaction_record WHERE tx_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(checkSql)) {
                pstmt.setString(1, txId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next() || !"TRY_SUCCESS".equals(rs.getString("status"))) {
                        conn.rollback();
                        return false;
                    }
                }
            }

            // 执行所有确认操作
            for (TccAction action : actions) {
                if (!action.confirmAction(conn, txId)) {
                    conn.rollback();
                    return false;
                }
            }

            // 更新事务状态为COMMITTED
            String updateSql =
                "UPDATE transaction_record SET status = ?, commit_time = ?, update_time = ? WHERE tx_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setString(1, "COMMITTED");
                pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                pstmt.setString(4, txId);
                pstmt.executeUpdate();
            }

            conn.commit();
            System.out.println("提交事务成功: " + txId);
            return true;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    /**
     * 回滚事务
     */
    public boolean rollback(Connection conn, String txId, TccAction... actions) throws SQLException {
        conn.setAutoCommit(false);

        try {
            // 执行所有取消操作
            for (TccAction action : actions) {
                action.cancelAction(conn, txId); // 即使失败也继续执行其他操作
            }

            // 更新事务状态为ROLLED_BACK
            String updateSql =
                "UPDATE transaction_record SET status = ?, rollback_time = ?, update_time = ? WHERE tx_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setString(1, "ROLLED_BACK");
                pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                pstmt.setString(4, txId);
                pstmt.executeUpdate();
            }

            conn.commit();
            System.out.println("回滚事务成功: " + txId);
            return true;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    /**
     * 检查事务是否已经完成
     */
    public boolean isTransactionCompleted(Connection conn, String txId) throws SQLException {
        String sql = "SELECT status FROM transaction_record WHERE tx_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String status = rs.getString("status");
                    return "COMMITTED".equals(status) || "ROLLED_BACK".equals(status);
                }
            }
        }
        return false;
    }
}
    