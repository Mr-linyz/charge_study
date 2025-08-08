package com.charging.message;

import com.alibaba.fastjson.JSON;
import com.charging.config.RabbitMQConfig;
import com.charging.model.PointsMessage;
import com.charging.service.impl.PointsService;
import com.charging.util.RabbitMQUtil;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 死信消息消费者，处理最终失败的消息
 */
@Slf4j
public class DeadLetterMessageConsumer {
    private final Channel channel;
    private final PointsService pointsService;
    private volatile boolean running = false;

    public DeadLetterMessageConsumer(PointsService pointsService) throws IOException, TimeoutException {
        this.pointsService = pointsService;
        this.channel = RabbitMQUtil.getChannel();
    }

    /**
     * 开始消费死信消息
     */
    public void startConsuming() throws IOException {
        if (running) {
            log.info("死信消费者已经在运行中");
            return;
        }

        running = true;

        // 创建消费者
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                AMQP.BasicProperties properties, byte[] body) throws IOException {
                String messageJson = new String(body, StandardCharsets.UTF_8);
                log.warn("收到死信消息: {}", messageJson);

                try {
                    // 解析消息
                    PointsMessage message = JSON.parseObject(messageJson, PointsMessage.class);

                    // 记录失败消息到数据库，等待人工干预
                    pointsService.recordFailedMessage(message, "超过最大重试次数，已放入死信队列");

                    // 确认消息
                    channel.basicAck(envelope.getDeliveryTag(), false);
                    log.info("死信消息已记录，消息ID: {}", message.getMessageId());

                } catch (Exception e) {
                    log.error("处理死信消息时发生异常", e);
                    // 死信消息也处理失败，仍然确认，避免无限循环
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        // 开始消费死信消息
        channel.basicConsume(RabbitMQConfig.getDeadLetterQueue(), false, consumer);
        log.info("死信消息消费者开始运行，监听队列: {}", RabbitMQConfig.getDeadLetterQueue());
    }

    /**
     * 停止消费死信消息
     */
    public void stopConsuming() {
        running = false;
        log.info("死信消息消费者已停止");
    }
}
    