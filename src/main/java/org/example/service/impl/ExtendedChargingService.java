package org.example.service.impl;

import com.google.gson.Gson;
import org.example.model.LocalMessage;
import org.example.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 扩展的充电服务类，增加了订单结算和消息创建功能
 */
public class ExtendedChargingService extends ChargingService {

    /**
     * 结算充电订单并创建积分消息（在同一个本地事务中）
     * @param orderId 订单ID
     * @return 是否成功
     */
    public boolean settleChargingOrder(String orderId) throws SQLException {
        try (Connection conn = DBUtil.getConnection()) {
            conn.setAutoCommit(false);
            
            try {
                // 1. 查询订单信息
                Map<String, Object> order = getOrder(conn, null, orderId);
                if (order == null) {
                    throw new SQLException("订单不存在: " + orderId);
                }
                
                String status = (String) order.get("status");
                if (!"COMPLETED".equals(status)) {
                    throw new SQLException("订单未完成，不能结算: " + orderId + ", 当前状态: " + status);
                }
                
                String userId = (String) order.get("user_id");
                double amount = (double) order.get("amount");
                
                // 2. 更新订单为已结算状态
                String updateOrderSql = "UPDATE charging_order SET settlement_status = 'SETTLED', settlement_time = ? " +
                                      "WHERE order_id = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(updateOrderSql)) {
                    pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                    pstmt.setString(2, orderId);
                    int rowsAffected = pstmt.executeUpdate();
                    if (rowsAffected != 1) {
                        throw new SQLException("更新订单结算状态失败");
                    }
                }
                
                // 3. 计算积分 (例如：每消费1元获得1积分)
                int points = (int) Math.round(amount);
                
                // 4. 创建本地消息表记录
                String messageId = "MSG_" + UUID.randomUUID().toString().replace("-", "");
                
                // 构建消息内容
                Map<String, Object> messageContent = new HashMap<>();
                messageContent.put("userId", userId);
                messageContent.put("orderId", orderId);
                messageContent.put("points", points);
                messageContent.put("createTime", new Timestamp(System.currentTimeMillis()));
                
                Gson gson = new Gson();
                String contentJson = gson.toJson(messageContent);
                
                LocalMessage message = new LocalMessage(
                    messageId, "CHARGING_ORDER_SETTLEMENT", orderId, contentJson);
                
                // 插入消息记录
                String insertMessageSql = "INSERT INTO local_message " +
                                        "(message_id, business_type, business_id, message_content, status, " +
                                        "retry_count, next_retry_time, create_time, update_time) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertMessageSql)) {
                    pstmt.setString(1, message.getMessageId());
                    pstmt.setString(2, message.getBusinessType());
                    pstmt.setString(3, message.getBusinessId());
                    pstmt.setString(4, message.getMessageContent());
                    pstmt.setString(5, message.getStatus());
                    pstmt.setInt(6, message.getRetryCount());
                    pstmt.setTimestamp(7, message.getNextRetryTime());
                    pstmt.setTimestamp(8, message.getCreateTime());
                    pstmt.setTimestamp(9, message.getUpdateTime());
                    pstmt.executeUpdate();
                }
                
                // 5. 提交事务
                conn.commit();
                System.out.println("订单结算完成并创建积分消息: 订单ID=" + orderId + ", 积分=" + points);
                return true;
            } catch (SQLException e) {
                conn.rollback();
                System.err.println("订单结算失败: " + e.getMessage());
                throw e;
            }
        }
    }
    
    /**
     * 根据订单ID查询订单（重写父类方法，增加订单ID查询）
     */
    public Map<String, Object> getOrder(Connection conn, String txId, String orderId) throws SQLException {
        String sql;
        if (orderId != null) {
            sql = "SELECT * FROM charging_order WHERE order_id = ?";
        } else {
            sql = "SELECT * FROM charging_order WHERE tx_id = ?";
        }
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            if (orderId != null) {
                pstmt.setString(1, orderId);
            } else {
                pstmt.setString(1, txId);
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> order = new HashMap<>();
                    order.put("order_id", rs.getString("order_id"));
                    order.put("tx_id", rs.getString("tx_id"));
                    order.put("charging_point_id", rs.getString("charging_point_id"));
                    order.put("user_id", rs.getString("user_id"));
                    order.put("amount", rs.getDouble("amount"));
                    order.put("status", rs.getString("status"));
                    order.put("settlement_status", rs.getString("settlement_status"));
                    return order;
                }
            }
        }
        return null;
    }
}
    