package com.charging.scheduler;

import com.charging.message.PointsMessageProducer;
import com.charging.model.PointsMessage;
import com.charging.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 消息处理器定时任务，扫描本地消息表并发送到消息队列
 */
@Slf4j
public class MessageProcessor {
    private final PointsMessageProducer messageProducer;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private int checkIntervalSeconds = 30; // 默认30秒检查一次
    private int maxRetryCount = 5; // 最大重试次数

    public MessageProcessor() {
        this.messageProducer = new PointsMessageProducer();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "MessageProcessor-Thread");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void setCheckIntervalSeconds(int seconds) {
        this.checkIntervalSeconds = seconds;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
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
        log.info("消息处理器启动，检查间隔: {}秒，最大重试次数: {}", checkIntervalSeconds, maxRetryCount);

        // 立即执行一次，然后按间隔执行
        scheduler.scheduleAtFixedRate(
            this::processPendingMessages,
            0,
            checkIntervalSeconds,
            TimeUnit.SECONDS
        );
    }

    /**
     * 停止定时任务
     */
    public void stop() {
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        log.info("消息处理器已停止");
    }

    /**
     * 处理待发送的消息
     */
    private void processPendingMessages() {
        if (!running) {
            return;
        }

        Connection conn = null;
        PreparedStatement selectStmt = null;
        PreparedStatement updateStmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            conn.setAutoCommit(false);

            // 查询待处理的消息（加锁防止并发处理，跳过已锁定的记录）
            String selectSql = "SELECT id, order_id, user_id, points, retry_count " +
                "FROM local_message " +
                "WHERE status = 'PENDING' " +
                "AND (retry_count < ? OR retry_count IS NULL) " +
                "ORDER BY create_time ASC " +
                "LIMIT 100 FOR UPDATE SKIP LOCKED";

            selectStmt = conn.prepareStatement(selectSql);
            selectStmt.setInt(1, maxRetryCount);
            rs = selectStmt.executeQuery();

            int processedCount = 0;

            // 处理每条消息
            while (rs.next()) {
                String messageId = rs.getString("id");
                String orderId = rs.getString("order_id");
                String userId = rs.getString("user_id");
                BigDecimal points = rs.getBigDecimal("points");
                int retryCount = rs.getInt("retry_count");

                try {
                    // 创建积分消息
                    PointsMessage message = PointsMessage.create(orderId, userId, points);
                    message.setMessageId(messageId); // 使用本地消息表的ID作为消息ID

                    // 发送消息到队列
                    messageProducer.sendPointsMessage(message);

                    // 更新消息状态为已发送
                    String updateSql = "UPDATE local_message " +
                        "SET status = 'SENT', send_time = NOW(), retry_count = ? " +
                        "WHERE id = ?";

                    updateStmt = conn.prepareStatement(updateSql);
                    updateStmt.setInt(1, retryCount + 1);
                    updateStmt.setString(2, messageId);
                    updateStmt.executeUpdate();

                    processedCount++;
                    log.info("消息 {} 发送成功，订单: {}, 第 {} 次尝试",
                        messageId, orderId, retryCount + 1);

                } catch (Exception e) {
                    log.error("消息 {} 处理失败，订单: {}", messageId, orderId, e);

                    // 更新重试次数
                    String updateRetrySql = "UPDATE local_message " +
                        "SET retry_count = ?, status = ? " +
                        "WHERE id = ?";

                    updateStmt = conn.prepareStatement(updateRetrySql);
                    updateStmt.setInt(1, retryCount + 1);

                    // 如果达到最大重试次数，标记为失败
                    String newStatus = (retryCount + 1 >= maxRetryCount) ? "FAILED" : "PENDING";
                    updateStmt.setString(2, newStatus);
                    updateStmt.setString(3, messageId);
                    updateStmt.executeUpdate();
                }
            }

            conn.commit();
            if (processedCount > 0) {
                log.info("本次处理完成，共发送 {} 条消息", processedCount);
            }

        } catch (SQLException e) {
            log.error("处理待发送消息时发生数据库错误", e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    log.error("回滚事务失败", ex);
                }
            }
        } finally {
            DBUtil.close(conn, selectStmt, rs);
            if (updateStmt != null) {
                try {
                    updateStmt.close();
                } catch (SQLException e) {
                    log.error("关闭PreparedStatement失败", e);
                }
            }
        }
    }
}
    