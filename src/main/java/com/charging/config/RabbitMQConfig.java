package com.charging.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * RabbitMQ配置类
 */
public class RabbitMQConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = RabbitMQConfig.class.getClassLoader().getResourceAsStream("rabbitmq.properties")) {
            if (input == null) {
                throw new RuntimeException("无法找到rabbitmq.properties配置文件");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("加载RabbitMQ配置失败", ex);
        }
    }

    public static String getHost() {
        return properties.getProperty("rabbitmq.host", "localhost");
    }

    public static int getPort() {
        return Integer.parseInt(properties.getProperty("rabbitmq.port", "5672"));
    }

    public static String getUsername() {
        return properties.getProperty("rabbitmq.username", "guest");
    }

    public static String getPassword() {
        return properties.getProperty("rabbitmq.password", "guest");
    }

    public static String getVirtualHost() {
        return properties.getProperty("rabbitmq.virtual-host", "/");
    }

    // 积分消息交换机
    public static String getPointsExchange() {
        return properties.getProperty("rabbitmq.points.exchange", "charging.points.exchange");
    }

    // 积分消息路由键
    public static String getPointsRoutingKey() {
        return properties.getProperty("rabbitmq.points.routing-key", "charging.points.routing.key");
    }

    // 积分消息队列
    public static String getPointsQueue() {
        return properties.getProperty("rabbitmq.points.queue", "charging.points.queue");
    }

    // 死信交换机
    public static String getDeadLetterExchange() {
        return properties.getProperty("rabbitmq.dlq.exchange", "charging.points.dlq.exchange");
    }

    // 死信路由键
    public static String getDeadLetterRoutingKey() {
        return properties.getProperty("rabbitmq.dlq.routing-key", "charging.points.dlq.routing.key");
    }

    // 死信队列
    public static String getDeadLetterQueue() {
        return properties.getProperty("rabbitmq.dlq.queue", "charging.points.dlq.queue");
    }
}
    