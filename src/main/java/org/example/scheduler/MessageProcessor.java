package org.example.scheduler;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.example.service.impl.PointsService;
import org.example.util.DBUtil;

import java.sql.*;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 消息处理器，定时扫描并处理本地消息表中的消息
 */
public class MessageProcessor {
    private PointsService pointsService;
    private long checkIntervalSeconds = 5; // 检查间隔，默认60秒
    private int maxRetryCount = 5; // 最大重试次数
    private Timer timer;

    public MessageProcessor(PointsService pointsService) {
        this.pointsService = pointsService;
    }

    /**
     * 启动消息处理器
     */
    public void start() {
        timer = new Timer("MessageProcessor", true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                processPendingMessages();
            }
        }, 0, checkIntervalSeconds * 1000);

        System.out.println("消息处理器已启动，检查间隔: " + checkIntervalSeconds + "秒");
    }

    /**
     * 停止消息处理器
     */
    public void stop() {
        if (timer != null) {
            timer.cancel();
            System.out.println("消息处理器已停止");
        }
    }

    /**
     * 处理待处理的消息
     */
    private void processPendingMessages() {
        System.out.println("\n开始处理待处理消息...");

        try (Connection conn = DBUtil.getConnection()) {
            // 查询待处理或需要重试的消息
            String sql =
                "SELECT * FROM local_message " + "WHERE (status = 'PENDING' OR status = 'FAILED') " + "AND next_retry_time <= ? " + "AND retry_count < ? " + "ORDER BY create_time ASC";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                pstmt.setInt(2, maxRetryCount);

                try (ResultSet rs = pstmt.executeQuery()) {
                    int messageCount = 0;

                    while (rs.next()) {
                        messageCount++;
                        String messageId = rs.getString("message_id");
                        String businessType = rs.getString("business_type");
                        String messageContent = rs.getString("message_content");
                        int retryCount = rs.getInt("retry_count");

                        System.out.println(
                            "处理消息: " + messageId + ", 业务类型: " + businessType + ", 重试次数: " + retryCount);

                        // 处理消息（此处是直接消费的，实际上是使用MQ去做异步的处理）
                        // todo 使用MQ去做异步的处理,使用MQ就要考虑更多了，消费者的幂等等问题
                        boolean processSuccess = processMessage(conn, messageId, businessType, messageContent);

                        if (!processSuccess) {
                            // 处理失败，更新重试信息（指数退避策略）
                            retryCount++;
                            long nextRetryDelay = (long)(Math.pow(2, retryCount) * 60 * 1000); // 2^retryCount分钟

                            updateMessageRetryInfo(conn, messageId, retryCount,
                                new Timestamp(System.currentTimeMillis() + nextRetryDelay));

                            if (retryCount >= maxRetryCount) {
                                markMessageAsFailed(conn, messageId, "达到最大重试次数");
                                System.err.println("消息处理失败且达到最大重试次数: " + messageId);
                            }
                        }
                    }

                    System.out.println("消息处理完成，共处理 " + messageCount + " 条消息");
                }
            }
        } catch (SQLException e) {
            System.err.println("处理消息时发生数据库错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 处理单个消息
     */
    private boolean processMessage(Connection conn, String messageId, String businessType, String messageContent) {
        try {
            // 根据业务类型处理不同的消息
            if ("CHARGING_ORDER_SETTLEMENT".equals(businessType)) {
                // 解析消息内容
                Gson gson = new Gson();
                Map<String, Object> content = gson.fromJson(messageContent, new TypeToken<Map<String, Object>>() {
                }.getType());

                String userId = (String)content.get("userId");
                String orderId = (String)content.get("orderId");
                double pointsDouble = (double)content.get("points");
                int points = (int)pointsDouble;

                // 调用积分服务增加积分
                boolean success = pointsService.addPointsForCharging(userId, orderId, points);

                if (success) {
                    // 标记消息为已确认
                    markMessageAsConfirmed(conn, messageId);
                    return true;
                }
            } else {
                System.err.println("未知的业务类型: " + businessType);
                markMessageAsFailed(conn, messageId, "未知的业务类型");
            }
        } catch (Exception e) {
            System.err.println("处理消息 " + messageId + " 失败: " + e.getMessage());
        }

        return false;
    }

    /**
     * 标记消息为已确认
     */
    private void markMessageAsConfirmed(Connection conn, String messageId) throws SQLException {
        String sql = "UPDATE local_message SET status = 'CONFIRMED', update_time = ? " + "WHERE message_id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmt.setString(2, messageId);
            pstmt.executeUpdate();
        }

        System.out.println("消息已确认处理成功: " + messageId);
    }

    /**
     * 标记消息为失败
     */
    private void markMessageAsFailed(Connection conn, String messageId, String reason) throws SQLException {
        String sql =
            "UPDATE local_message SET status = 'FAILED', update_time = ?, remark = ? " + "WHERE message_id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmt.setString(2, reason);
            pstmt.setString(3, messageId);
            pstmt.executeUpdate();
        }
    }

    /**
     * 更新消息重试信息
     */
    private void updateMessageRetryInfo(Connection conn, String messageId, int retryCount, Timestamp nextRetryTime)
        throws SQLException {
        String sql =
            "UPDATE local_message SET status = 'FAILED', retry_count = ?, next_retry_time = ?, " + "update_time = ? WHERE message_id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, retryCount);
            pstmt.setTimestamp(2, nextRetryTime);
            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            pstmt.setString(4, messageId);
            pstmt.executeUpdate();
        }

        System.out.println("消息将重试: " + messageId + ", 下次重试时间: " + nextRetryTime);
    }

    // Setters
    public void setCheckIntervalSeconds(long checkIntervalSeconds) {
        this.checkIntervalSeconds = checkIntervalSeconds;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }
}
    