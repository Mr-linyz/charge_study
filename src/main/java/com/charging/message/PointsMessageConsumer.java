package com.charging.message;

import com.alibaba.fastjson.JSON;
import com.charging.model.PointsMessage;
import com.charging.service.PointsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 积分消息消费者，监听积分消息队列并处理积分增加
 */
@Slf4j
@Component
public class PointsMessageConsumer {

    private final PointsService pointsService;

    @Autowired
    public PointsMessageConsumer(PointsService pointsService) {
        this.pointsService = pointsService;
    }

    /**
     * 监听积分消息队列
     */
    @RabbitListener(queues = "${app.rabbit.points.queue.name}")
    public void handlePointsMessage(String messageJson) {
        try {
            log.info("收到积分消息: {}", messageJson);

            // 解析消息
            PointsMessage message = JSON.parseObject(messageJson, PointsMessage.class);
            if (message == null) {
                log.error("解析积分消息失败，消息内容: {}", messageJson);
                return;
            }

            // 调用积分服务增加积分
            boolean success = pointsService.addPoints(
                message.getOrderId(),
                message.getUserId(),
                message.getPoints().doubleValue()
            );

            if (success) {
                log.info("积分增加成功, messageId: {}, orderId: {}, userId: {}, points: {}",
                    message.getMessageId(), message.getOrderId(),
                    message.getUserId(), message.getPoints());
            } else {
                log.error("积分增加失败, messageId: {}, orderId: {}",
                    message.getMessageId(), message.getOrderId());
                // 消费失败会触发重试机制
                throw new RuntimeException("积分增加处理失败，将触发重试");
            }
        } catch (Exception e) {
            log.error("处理积分消息异常", e);
            // 抛出异常，让RabbitMQ进行重试
            throw new RuntimeException("处理积分消息异常", e);
        }
    }
}
    