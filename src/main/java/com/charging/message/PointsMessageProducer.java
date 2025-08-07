package com.charging.message;

import com.alibaba.fastjson.JSON;
import com.charging.model.PointsMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 积分消息生产者，负责将积分消息发送到RabbitMQ队列
 */
@Slf4j
@Component
public class PointsMessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final String pointsExchange;
    private final String pointsRoutingKey;

    @Autowired
    public PointsMessageProducer(RabbitTemplate rabbitTemplate,
        @Value("${app.rabbit.points.exchange}") String pointsExchange,
        @Value("${app.rabbit.points.routing-key}") String pointsRoutingKey) {
        this.rabbitTemplate = rabbitTemplate;
        this.pointsExchange = pointsExchange;
        this.pointsRoutingKey = pointsRoutingKey;

        // 设置消息确认回调
        this.rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                log.info("消息发送成功, messageId: {}", correlationData.getId());
            } else {
                log.error("消息发送失败, messageId: {}, 原因: {}", correlationData.getId(), cause);
                // 可以在这里实现消息发送失败的重试逻辑
            }
        });
    }

    /**
     * 发送积分消息到队列
     */
    public void sendPointsMessage(String orderId, String userId, double points) {
        try {
            // 创建积分消息
            PointsMessage message = PointsMessage.build(orderId, userId, points);

            // 创建消息关联数据，用于确认回调
            CorrelationData correlationData = new CorrelationData(message.getMessageId());

            // 发送消息
            rabbitTemplate.convertAndSend(pointsExchange, pointsRoutingKey, JSON.toJSONString(message),
                correlationData);

            log.info("已发送积分消息, messageId: {}, orderId: {}, userId: {}, points: {}", message.getMessageId(),
                orderId, userId, points);
        } catch (Exception e) {
            log.error("发送积分消息异常, orderId: {}, userId: {}", orderId, userId, e);
            throw new RuntimeException("发送积分消息失败", e);
        }
    }
}
    