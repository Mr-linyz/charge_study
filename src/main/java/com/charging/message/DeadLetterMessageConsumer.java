package com.charging.message;

import com.alibaba.fastjson.JSON;
import com.charging.model.PointsMessage;
import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 死信消息消费者，处理最终失败的积分消息
 */
@Slf4j
@Component
public class DeadLetterMessageConsumer {

    /**
     * 监听死信队列，处理最终失败的消息
     */
    @RabbitListener(queues = "${app.rabbit.points.queue.dead-letter-queue}")
    public void handleDeadLetterMessage(PointsMessage message) {
        log.error("接收到死信消息(最终失败): {}", JSON.toJSONString(message));

        // 将失败消息记录到数据库，等待人工干预
        String sql = "INSERT INTO failed_points_message " +
            "(message_id, order_id, user_id, points, error_message, create_time) " +
            "VALUES (?, ?, ?, ?, ?, NOW())";

        try (Connection conn = DBUtil.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, message.getMessageId());
            stmt.setString(2, message.getOrderId());
            stmt.setString(3, message.getUserId());
            stmt.setDouble(4, message.getPoints());
            stmt.setString(5, "消息经过多次重试仍处理失败，已进入死信队列");

            stmt.executeUpdate();
            log.info("死信消息已记录到数据库，等待人工处理: {}", message.getOrderId());

        } catch (SQLException e) {
            log.error("记录死信消息到数据库失败: {}", e.getMessage(), e);
        }
    }
}
    