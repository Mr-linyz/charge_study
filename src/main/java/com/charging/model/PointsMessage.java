package com.charging.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 积分消息实体类，用于在消息队列中传输
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PointsMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    // 消息唯一ID
    private String messageId;

    // 订单ID
    private String orderId;

    // 用户ID
    private String userId;

    // 积分数量
    private Double points;

    // 消息创建时间
    private LocalDateTime createTime;

    /**
     * 构建积分消息的静态方法
     */
    public static PointsMessage build(String orderId, String userId, Double points) {
        PointsMessage message = new PointsMessage();
        message.setMessageId(UUID.randomUUID().toString());
        message.setOrderId(orderId);
        message.setUserId(userId);
        message.setPoints(points);
        message.setCreateTime(LocalDateTime.now());
        return message;
    }
}
    