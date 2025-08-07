package com.charging.scheduler;

import com.charging.message.PointsMessageProducer;
import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * 消息处理器定时任务，扫描本地消息表并发送到消息队列
 */
@Slf4j
@Component
public class MessageProcessor {

    private final PointsMessageProducer messageProducer;
    private final int checkIntervalSeconds;
    private volatile boolean running = false;

    @Autowired
    public MessageProcessor(PointsMessageProducer messageProducer,
        @Value("${app.scheduler.message-check-interval}") int checkIntervalSeconds) {
        this.messageProducer = messageProducer;
        this.checkIntervalSeconds = checkIntervalSeconds / 1000; // 转换为秒
    }

    /**
     * 启动定时任务
     */
    public void start() {
        if (running) {
            log.info("消息处理器已经在运行中");
            return;
        }

        running = true;
        new Thread(this::processMessages, "PointsMessageProcessor").start();
        log.info("消息处理器已启动，检查间隔: {}秒", checkIntervalSeconds);
    }

    /**
     * 停止定时任务
     */
    public void stop() {
        running = false;
        log.info("消息处理器已停止");
    }

    /**
     * 处理消息的核心逻辑
     */
    private void processMessages() {
        while (running) {
            try {
                // 扫描并处理待发送的消息
                scanAndSendMessages();

                // 等待下一次检查
                TimeUnit.SECONDS.sleep(checkIntervalSeconds);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
                log.info("消息处理器线程被中断");
            } catch (Exception e) {
                log.error("消息处理异常: {}", e.getMessage(), e);
                try {
                    // 发生异常时延长等待时间
                    TimeUnit.SECONDS.sleep(Math.min(checkIntervalSeconds * 2, 60));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }
    }

    /**
     * 扫描本地消息表并发送消息到队列
     */
    private void scanAndSendMessages() {
        // 查询待处理的消息，加行锁防止并发处理，跳过已锁定的记录
        String selectSql = "SELECT id, order_id, user_id, points " +
            "FROM local_message " +
            "WHERE status = 'PENDING' " +
            "AND retry_count < 5 " + // 最多重试5次
            "LIMIT 100 FOR UPDATE SKIP LOCKED";

        // 更新消息状态为已发送
        String updateSql = "UPDATE local_message " +
            "SET status = 'SENT', send_time = NOW() " +
            "WHERE id = ?";

        try (Connection conn = DBUtil.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
                PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

                ResultSet rs = selectStmt.executeQuery();

                int sentCount = 0;
                while (rs.next()) {
                    String messageId = rs.getString("id");
                    String orderId = rs.getString("order_id");
                    String userId = rs.getString("user_id");
                    double points = rs.getDouble("points");

                    try {
                        // 发送消息到队列
                        messageProducer.sendPointsMessage(orderId, userId, points);

                        // 更新消息状态为已发送
                        updateStmt.setString(1, messageId);
                        updateStmt.executeUpdate();

                        sentCount++;
                        log.info("消息 {} 发送成功，订单: {}", messageId, orderId);
                    } catch (Exception e) {
                        log.error("消息 {} 发送失败: {}", messageId, e.getMessage());
                        // 不回滚整个事务，继续处理下一条消息
                    }
                }

                conn.commit();
                if (sentCount > 0) {
                    log.info("成功发送 {} 条积分消息到队列", sentCount);
                }
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("扫描本地消息表异常: {}", e.getMessage(), e);
        }
    }
}
    