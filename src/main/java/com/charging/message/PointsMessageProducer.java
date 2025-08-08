package com.charging.message;

import com.alibaba.fastjson.JSON;
import com.charging.model.PointsMessage;
import com.charging.util.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.charging.config.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 积分消息生产者
 */
@Slf4j
public class PointsMessageProducer {

    /**
     * 发送积分消息
     */
    public void sendPointsMessage(PointsMessage message) throws IOException, TimeoutException {
        if (message == null) {
            log.error("发送的消息不能为空");
            throw new IllegalArgumentException("消息不能为空");
        }

        try (Channel channel = RabbitMQUtil.getChannel()) {
            // 消息内容
            String messageJson = JSON.toJSONString(message);

            // 发送消息
            channel.basicPublish(
                RabbitMQConfig.getPointsExchange(),
                RabbitMQConfig.getPointsRoutingKey(),
                null,
                messageJson.getBytes("UTF-8")
            );

            log.info("积分消息发送成功，消息ID: {}, 订单ID: {}, 用户ID: {}, 积分: {}",
                message.getMessageId(), message.getOrderId(),
                message.getUserId(), message.getPoints());
        }
    }
}
    