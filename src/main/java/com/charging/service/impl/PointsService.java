package com.charging.service.impl;

import com.charging.model.PointsMessage;
import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 积分服务类，处理积分相关业务逻辑
 */
@Slf4j
public class PointsService {

    /**
     * 为用户增加积分（确保幂等性）
     * @param orderId 订单ID，用于确保幂等性
     * @param userId 用户ID
     * @param points 积分数量
     * @return 是否成功
     */
    public boolean addPoints(String orderId, String userId, BigDecimal points) {
        Connection conn = null;
        try {
            conn = DBUtil.getConnection();
            conn.setAutoCommit(false);

            // 1. 检查该订单是否已经处理过（幂等性检查）
            List<Map<String, Object>> transactions = DBUtil.executeQuery(
                conn,
                "SELECT id FROM points_transaction WHERE order_id = ?",
                orderId
            );

            if (!transactions.isEmpty()) {
                log.info("订单 {} 已处理过积分，无需重复处理", orderId);
                conn.commit();
                return true;
            }

            // 2. 检查用户是否存在，不存在则创建
            List<Map<String, Object>> userPoints = DBUtil.executeQuery(
                conn,
                "SELECT total_points FROM user_points WHERE user_id = ?",
                userId
            );

            if (userPoints.isEmpty()) {
                DBUtil.executeUpdate(
                    conn,
                    "INSERT INTO user_points (user_id, total_points) VALUES (?, ?)",
                    userId, points
                );
                log.info("用户 {} 首次添加积分，初始积分: {}", userId, points);
            } else {
                // 3. 更新用户积分
                BigDecimal currentPoints = (BigDecimal) userPoints.get(0).get("total_points");
                BigDecimal newPoints = currentPoints.add(points);

                DBUtil.executeUpdate(
                    conn,
                    "UPDATE user_points SET total_points = ? WHERE user_id = ?",
                    newPoints, userId
                );
                log.info("用户 {} 积分更新，原积分: {}, 新增: {}, 新积分: {}",
                    userId, currentPoints, points, newPoints);
            }

            // 4. 记录积分交易明细
            DBUtil.executeUpdate(
                conn,
                "INSERT INTO points_transaction (user_id, order_id, points) VALUES (?, ?, ?)",
                userId, orderId, points
            );

            // 5. 提交事务
            conn.commit();
            log.info("订单 {} 积分增加成功，用户: {}, 积分: {}", orderId, userId, points);
            return true;

        } catch (SQLException e) {
            log.error("增加积分失败，订单: {}, 用户: {}", orderId, userId, e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    log.error("回滚事务失败", ex);
                }
            }
            return false;
        } finally {
            DBUtil.close(conn, null, null);
        }
    }

    /**
     * 获取用户当前积分
     */
    public BigDecimal getUserPoints(String userId) {
        Connection conn = null;
        try {
            conn = DBUtil.getConnection();
            List<Map<String, Object>> result = DBUtil.executeQuery(
                conn,
                "SELECT total_points FROM user_points WHERE user_id = ?",
                userId
            );

            if (result.isEmpty()) {
                return BigDecimal.ZERO;
            }

            return (BigDecimal) result.get(0).get("total_points");

        } catch (SQLException e) {
            log.error("查询用户 {} 积分失败", userId, e);
            return BigDecimal.ZERO;
        } finally {
            DBUtil.close(conn, null, null);
        }
    }

    /**
     * 记录失败的积分消息
     */
    public void recordFailedMessage(PointsMessage message, String errorMessage) {
        Connection conn = null;
        try {
            conn = DBUtil.getConnection();
            DBUtil.executeUpdate(
                conn,
                "INSERT INTO failed_points_message (message_id, order_id, user_id, points, error_message) " +
                    "VALUES (?, ?, ?, ?, ?)",
                message.getMessageId(),
                message.getOrderId(),
                message.getUserId(),
                message.getPoints(),
                errorMessage
            );
            log.info("已记录失败消息，消息ID: {}", message.getMessageId());
        } catch (SQLException e) {
            log.error("记录失败消息失败", e);
        } finally {
            DBUtil.close(conn, null, null);
        }
    }
}
    