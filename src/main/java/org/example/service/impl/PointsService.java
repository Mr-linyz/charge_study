package org.example.service.impl;

import org.example.model.PointsTransaction;
import org.example.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * 积分服务类，处理用户积分相关操作
 */
public class PointsService {

    /**
     * 为用户增加积分（幂等性实现）
     * @param userId 用户ID
     * @param orderId 订单ID
     * @param points 积分数量
     * @return 是否成功
     */
    public boolean addPointsForCharging(String userId, String orderId, int points) throws SQLException {
        // 使用订单ID作为幂等键，确保同一订单不会重复增加积分
        String transactionId = "PTS_" + orderId;
        
        try (Connection conn = DBUtil.getConnection()) {
            conn.setAutoCommit(false);
            
            // 检查是否已经处理过该订单的积分
            if (isPointsAddedForOrder(conn, orderId)) {
                System.out.println("订单已添加过积分，无需重复处理: " + orderId);
                conn.commit();
                return true;
            }
            
            // 创建积分交易记录
            PointsTransaction transaction = new PointsTransaction(
                transactionId, userId, orderId, points, "CHARGING_REWARD");
            
            // 插入积分交易记录
            String insertTxSql = "INSERT INTO points_transaction " +
                               "(transaction_id, user_id, order_id, points, type, status, create_time) " +
                               "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertTxSql)) {
                pstmt.setString(1, transaction.getTransactionId());
                pstmt.setString(2, transaction.getUserId());
                pstmt.setString(3, transaction.getOrderId());
                pstmt.setInt(4, transaction.getPoints());
                pstmt.setString(5, transaction.getType());
                pstmt.setString(6, transaction.getStatus());
                pstmt.setTimestamp(7, transaction.getCreateTime());
                pstmt.executeUpdate();
            }
            
            // 更新用户积分（如果用户积分记录不存在则创建）
            String mergeSql = "INSERT INTO user_points (user_id, total_points, update_time) " +
                            "VALUES (?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE total_points = total_points + ?, update_time = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(mergeSql)) {
                pstmt.setString(1, userId);
                pstmt.setInt(2, points);
                pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                pstmt.setInt(4, points);
                pstmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                pstmt.executeUpdate();
            }
            
            // 更新积分交易状态为完成
            String updateTxSql = "UPDATE points_transaction SET status = 'COMPLETED', complete_time = ? " +
                               "WHERE transaction_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateTxSql)) {
                pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                pstmt.setString(2, transactionId);
                pstmt.executeUpdate();
            }
            
            conn.commit();
            System.out.println("用户积分增加成功: 用户ID=" + userId + ", 订单ID=" + orderId + ", 积分=" + points);
            return true;
        } catch (SQLException e) {
            throw e;
        }
    }
    
    /**
     * 检查订单是否已经添加过积分
     */
    private boolean isPointsAddedForOrder(Connection conn, String orderId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM points_transaction " +
                   "WHERE order_id = ? AND type = 'CHARGING_REWARD' AND status = 'COMPLETED'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, orderId);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    /**
     * 获取用户当前积分
     */
    public int getUserPoints(Connection conn, String userId) throws SQLException {
        String sql = "SELECT total_points FROM user_points WHERE user_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, userId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("total_points");
                }
                return 0;
            }
        }
    }
}
    