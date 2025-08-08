package com.charging.message;

import com.alibaba.fastjson.JSON;
import com.charging.config.RabbitMQConfig;
import com.charging.model.PointsMessage;
import com.charging.service.impl.PointsService;
import com.charging.util.RabbitMQUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 积分消息消费者
 */
@Slf4j
public class PointsMessageConsumer {
    private final Channel channel;
    private final PointsService pointsService;
    private volatile boolean running = false;

    public PointsMessageConsumer(PointsService pointsService) throws IOException, TimeoutException {
        this.pointsService = pointsService;
        this.channel = RabbitMQUtil.getChannel();

        // 设置每次只消费一条消息，处理完成后再接收下一条
        channel.basicQos(1);
    }

    /**
     * 开始消费消息
     */
    public void startConsuming() throws IOException {
        if (running) {
            log.info("消费者已经在运行中");
            return;
        }

        running = true;

        // 创建消费者
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                byte[] body) throws IOException {
                String messageJson = new String(body, StandardCharsets.UTF_8);
                log.info("收到积分消息: {}", messageJson);

                try {
                    // 解析消息
                    PointsMessage message = JSON.parseObject(messageJson, PointsMessage.class);

                    // 处理积分增加
                    boolean success =
                        pointsService.addPoints(message.getOrderId(), message.getUserId(), message.getPoints());

                    if (success) {
                        // 处理成功，确认消息
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        log.info("消息处理成功，消息ID: {}", message.getMessageId());
                    } else {
                        // 处理失败，拒绝消息并将其放入死信队列
                        channel.basicReject(envelope.getDeliveryTag(), false);
                        log.error("消息处理失败，将被放入死信队列，消息ID: {}", message.getMessageId());
                    }

                } catch (Exception e) {
                    log.error("处理消息时发生异常", e);
                    // 处理异常，拒绝消息并将其放入死信队列
                    channel.basicReject(envelope.getDeliveryTag(), false);
                }
            }
        };

        // 开始消费消息，手动确认模式
        channel.basicConsume(RabbitMQConfig.getPointsQueue(), false, consumer);
        log.info("积分消息消费者开始运行，监听队列: {}", RabbitMQConfig.getPointsQueue());
    }

    /**
     * 停止消费消息
     */
    public void stopConsuming() {
        running = false;
        log.info("积分消息消费者已停止");
    }
}
    