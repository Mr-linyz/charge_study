package com.charging.config;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ队列配置类
 */
@Configuration
public class RabbitMQConfig {

    // 正常队列配置
    @Value("${app.rabbit.points.exchange}")
    private String pointsExchange;

    @Value("${app.rabbit.points.routing-key}")
    private String pointsRoutingKey;

    @Value("${app.rabbit.points.queue.name}")
    private String pointsQueueName;

    // 死信队列配置
    @Value("${app.rabbit.points.queue.dead-letter-exchange}")
    private String deadLetterExchange;

    @Value("${app.rabbit.points.queue.dead-letter-routing-key}")
    private String deadLetterRoutingKey;

    @Value("${app.rabbit.points.queue.dead-letter-queue}")
    private String deadLetterQueueName;

    @Value("${app.rabbit.points.queue.ttl}")
    private long messageTTL;

    /**
     * 声明积分消息交换机
     */
    @Bean
    public DirectExchange pointsExchange() {
        return ExchangeBuilder.directExchange(pointsExchange)
            .durable(true)
            .build();
    }

    /**
     * 声明死信交换机
     */
    @Bean
    public DirectExchange deadLetterExchange() {
        return ExchangeBuilder.directExchange(deadLetterExchange)
            .durable(true)
            .build();
    }

    /**
     * 声明积分消息队列
     */
    @Bean
    public Queue pointsQueue() {
        // 设置队列参数，指定死信交换机和路由键
        Map<String, Object> arguments = new HashMap<>(3);
        arguments.put("x-dead-letter-exchange", deadLetterExchange);
        arguments.put("x-dead-letter-routing-key", deadLetterRoutingKey);
        arguments.put("x-message-ttl", messageTTL); // 消息过期时间

        return QueueBuilder.durable(pointsQueueName)
            .withArguments(arguments)
            .build();
    }

    /**
     * 声明死信队列
     */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(deadLetterQueueName)
            .build();
    }

    /**
     * 绑定积分队列到交换机
     */
    @Bean
    public Binding pointsBinding() {
        return BindingBuilder.bind(pointsQueue())
            .to(pointsExchange())
            .with(pointsRoutingKey);
    }

    /**
     * 绑定死信队列到死信交换机
     */
    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
            .to(deadLetterExchange())
            .with(deadLetterRoutingKey);
    }
}
    