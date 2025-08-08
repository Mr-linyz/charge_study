package com.charging.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * 积分消息实体类
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
    private BigDecimal points;

    // 创建时间戳
    private long createTime;

    /**
     * 创建积分消息
     */
    public static PointsMessage create(String orderId, String userId, BigDecimal points) {
        PointsMessage message = new PointsMessage();
        message.setMessageId(UUID.randomUUID().toString());
        message.setOrderId(orderId);
        message.setUserId(userId);
        message.setPoints(points);
        message.setCreateTime(System.currentTimeMillis());
        return message;
    }
}
    