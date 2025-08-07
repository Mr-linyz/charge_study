package com.charging.service;

import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * 订单结算服务，负责订单结算并创建积分消息
 */
@Slf4j
@Service
public class OrderSettlementService {

    /**
     * 结算充电订单并创建积分消息
     * 订单金额的1%将转换为用户积分
     */
    public boolean settleOrder(String orderId, String userId, double orderAmount) {
        log.info("开始结算订单: {}, 用户: {}, 金额: {}", orderId, userId, orderAmount);

        // 计算积分（订单金额的1%）
        double points = Math.round(orderAmount * 0.01 * 100) / 100.0;

        // 本地消息表插入SQL
        String insertMessageSql = "INSERT INTO local_message " +
            "(id, order_id, user_id, points, status, create_time) " +
            "VALUES (?, ?, ?, ?, 'PENDING', NOW())";

        // 模拟更新订单状态SQL
        String updateOrderSql = "UPDATE charging_order " +
            "SET status = 'SETTLED', settlement_time = NOW() " +
            "WHERE order_id = ?";

        try (Connection conn = DBUtil.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement orderStmt = conn.prepareStatement(updateOrderSql);
                PreparedStatement messageStmt = conn.prepareStatement(insertMessageSql)) {

                // 1. 更新订单状态为已结算
                orderStmt.setString(1, orderId);
                int orderUpdateCount = orderStmt.executeUpdate();
                if (orderUpdateCount == 0) {
                    log.error("订单 {} 不存在或已结算", orderId);
                    conn.rollback();
                    return false;
                }

                // 2. 插入积分消息
                String messageId = UUID.randomUUID().toString();
                messageStmt.setString(1, messageId);
                messageStmt.setString(2, orderId);
                messageStmt.setString(3, userId);
                messageStmt.setDouble(4, points);
                messageStmt.executeUpdate();

                // 提交事务
                conn.commit();
                log.info("订单 {} 结算成功，将为用户 {} 增加 {} 积分", orderId, userId, points);
                return true;

            } catch (SQLException e) {
                conn.rollback();
                log.error("订单 {} 结算失败: {}", orderId, e.getMessage(), e);
                return false;
            }
        } catch (SQLException e) {
            log.error("获取数据库连接失败: {}", e.getMessage(), e);
            return false;
        }
    }
}
    