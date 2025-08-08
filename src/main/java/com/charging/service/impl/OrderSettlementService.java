package com.charging.service.impl;

import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/**
 * 订单结算服务类
 */
@Slf4j
public class OrderSettlementService {

    // 积分比例：订单金额的1%转为积分
    private static final BigDecimal POINTS_RATIO = new BigDecimal("0.01");

    /**
     * 结算充电订单并创建积分消息
     *
     * @param orderId
     *     订单ID
     * @param userId
     *     用户ID
     * @param orderAmount
     *     订单金额
     * @return 是否结算成功
     */
    public boolean settleOrder(String orderId, String userId, BigDecimal orderAmount) {
        Connection conn = null;
        try {
            conn = DBUtil.getConnection();
            conn.setAutoCommit(false);

            // 1. 模拟更新订单状态为"已结算"
            // 实际项目中这里会更新订单表的状态
            log.info("订单 {} 开始结算，用户: {}, 金额: {}", orderId, userId, orderAmount);

            // 2. 计算应得积分 (订单金额 * 1%)
            BigDecimal points = orderAmount.multiply(POINTS_RATIO).setScale(2, BigDecimal.ROUND_HALF_UP);

            // 3. 创建本地消息表记录
            String messageId = UUID.randomUUID().toString();
            int rows = DBUtil.executeUpdate(conn,
                "INSERT INTO local_message (id, order_id, user_id, points, status) " + "VALUES (?, ?, ?, ?, 'PENDING')",
                messageId, orderId, userId, points);

            if (rows <= 0) {
                throw new SQLException("创建积分消息失败");
            }

            // 4. 提交事务
            conn.commit();
            log.info("订单 {} 结算成功，将为用户 {} 增加 {} 积分，消息ID: {}", orderId, userId, points, messageId);
            return true;

        } catch (SQLException e) {
            log.error("订单 {} 结算失败", orderId, e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    log.error("回滚订单结算事务失败", ex);
                }
            }
            return false;
        } finally {
            DBUtil.close(conn, null, null);
        }
    }
}
    