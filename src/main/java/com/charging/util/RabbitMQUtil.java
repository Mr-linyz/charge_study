package com.charging.util;

import com.charging.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ工具类
 */
@Slf4j
public class RabbitMQUtil {
    private static ConnectionFactory factory;
    private static volatile Connection connection;

    static {
        // 初始化连接工厂
        factory = new ConnectionFactory();
        factory.setHost(RabbitMQConfig.getHost());
        factory.setPort(RabbitMQConfig.getPort());
        factory.setUsername(RabbitMQConfig.getUsername());
        factory.setPassword(RabbitMQConfig.getPassword());
        factory.setVirtualHost(RabbitMQConfig.getVirtualHost());

        // 初始化连接
        try {
            getConnection();
            // 初始化队列
            initQueues();
            log.info("RabbitMQ初始化成功");
        } catch (IOException | TimeoutException e) {
            log.error("RabbitMQ初始化失败", e);
            throw new RuntimeException("RabbitMQ初始化失败", e);
        }
    }

    /**
     * 获取连接
     */
    public static Connection getConnection() throws IOException, TimeoutException {
        // 双检索同时保证效率和安全
        if (connection == null || !connection.isOpen()) {
            synchronized (RabbitMQUtil.class) {
                if (connection == null || !connection.isOpen()) {
                    connection = factory.newConnection();
                }
            }
        }
        return connection;
    }

    /**
     * 获取通道
     */
    public static Channel getChannel() throws IOException, TimeoutException {
        return getConnection().createChannel();
    }

    /**
     * 初始化队列和交换机
     */
    private static void initQueues() throws IOException, TimeoutException {
        try (Channel channel = getChannel()) {
            // 声明正常交换机
            channel.exchangeDeclare(RabbitMQConfig.getPointsExchange(), "direct", true, false, null);

            // 声明死信交换机
            channel.exchangeDeclare(RabbitMQConfig.getDeadLetterExchange(), "direct", true, false, null);

            // 设置正常队列参数，指定死信交换机和路由键
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put("x-dead-letter-exchange", RabbitMQConfig.getDeadLetterExchange());
            queueArgs.put("x-dead-letter-routing-key", RabbitMQConfig.getDeadLetterRoutingKey());
            queueArgs.put("x-message-ttl", 60000); // 消息过期时间：60秒

            // 声明正常队列
            channel.queueDeclare(RabbitMQConfig.getPointsQueue(), true, false, false, queueArgs);

            // 声明死信队列
            channel.queueDeclare(RabbitMQConfig.getDeadLetterQueue(), true, false, false, null);

            // 绑定正常队列到正常交换机
            channel.queueBind(RabbitMQConfig.getPointsQueue(), RabbitMQConfig.getPointsExchange(),
                RabbitMQConfig.getPointsRoutingKey());

            // 绑定死信队列到死信交换机
            channel.queueBind(RabbitMQConfig.getDeadLetterQueue(), RabbitMQConfig.getDeadLetterExchange(),
                RabbitMQConfig.getDeadLetterRoutingKey());
        }
    }

    /**
     * 关闭连接
     */
    public static void close() {
        if (connection != null) {
            try {
                connection.close();
                log.info("RabbitMQ连接已关闭");
            } catch (IOException e) {
                log.error("关闭RabbitMQ连接失败", e);
            }
        }
    }
}
    