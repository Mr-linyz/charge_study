package com.charging.service;

import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 积分服务，负责用户积分的增加和查询
 */
@Slf4j
@Service
public class PointsService {

    /**
     * 为用户增加积分（确保幂等性）
     * @param orderId 订单ID，用于确保幂等性
     * @param userId 用户ID
     * @param points 积分数量
     * @return 是否成功
     */
    public boolean addPoints(String orderId, String userId, double points) {
        log.info("为用户 {} 增加积分，订单: {}, 积分: {}", userId, orderId, points);

        // 检查该订单是否已经增加过积分（幂等性检查）
        if (hasOrderAddedPoints(orderId)) {
            log.warn("订单 {} 已增加过积分，无需重复处理", orderId);
            return true;
        }

        // SQL语句
        String insertTransactionSql = "INSERT INTO points_transaction " +
            "(user_id, order_id, points, transaction_time) " +
            "VALUES (?, ?, ?, NOW())";

        String updateUserPointsSql = "INSERT INTO user_points (user_id, total_points) " +
            "VALUES (?, ?) " +
            "ON DUPLICATE KEY UPDATE " +
            "total_points = total_points + ?, " +
            "update_time = NOW()";

        String updateMessageSql = "UPDATE local_message " +
            "SET status = 'PROCESSED', process_time = NOW() " +
            "WHERE order_id = ?";

        try (Connection conn = DBUtil.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement transactionStmt = conn.prepareStatement(insertTransactionSql);
                PreparedStatement userStmt = conn.prepareStatement(updateUserPointsSql);
                PreparedStatement messageStmt = conn.prepareStatement(updateMessageSql)) {

                // 1. 插入积分交易记录
                transactionStmt.setString(1, userId);
                transactionStmt.setString(2, orderId);
                transactionStmt.setDouble(3, points);
                transactionStmt.executeUpdate();

                // 2. 更新用户总积分
                userStmt.setString(1, userId);
                userStmt.setDouble(2, points);
                userStmt.setDouble(3, points);
                userStmt.executeUpdate();

                // 3. 更新消息状态为已处理
                messageStmt.setString(1, orderId);
                messageStmt.executeUpdate();

                // 提交事务
                conn.commit();
                log.info("用户 {} 积分增加成功，订单: {}, 增加积分: {}", userId, orderId, points);
                return true;

            } catch (SQLException e) {
                conn.rollback();
                log.error("用户 {} 积分增加失败: {}", userId, e.getMessage(), e);
                return false;
            }
        } catch (SQLException e) {
            log.error("获取数据库连接失败: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 检查订单是否已经增加过积分
     */
    private boolean hasOrderAddedPoints(String orderId) {
        String sql = "SELECT COUNT(1) FROM points_transaction WHERE order_id = ?";

        try (Connection conn = DBUtil.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, orderId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            log.error("检查订单积分状态失败: {}", e.getMessage(), e);
        }

        return false;
    }

    /**
     * 查询用户当前积分
     */
    public double getUserPoints(String userId) {
        String sql = "SELECT total_points FROM user_points WHERE user_id = ?";

        try (Connection conn = DBUtil.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, userId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getDouble("total_points");
            }
        } catch (SQLException e) {
            log.error("查询用户积分失败: {}", e.getMessage(), e);
        }

        return 0.0;
    }

    /**
     * 获取用户积分交易明细
     */
    public Map<String, Double> getTransactionDetails(String userId) {
        String sql = "SELECT order_id, points FROM points_transaction " +
            "WHERE user_id = ? ORDER BY transaction_time DESC";

        Map<String, Double> details = new TreeMap<>();

        try (Connection conn = DBUtil.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, userId);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                details.put(rs.getString("order_id"), rs.getDouble("points"));
            }
        } catch (SQLException e) {
            log.error("查询积分明细失败: {}", e.getMessage(), e);
        }

        return details;
    }
}
    