package com.charging;

import com.charging.message.DeadLetterMessageConsumer;
import com.charging.message.PointsMessageConsumer;
import com.charging.scheduler.MessageProcessor;
import com.charging.service.impl.OrderSettlementService;
import com.charging.service.impl.PointsService;
import com.charging.util.DBUtil;
import com.charging.util.RabbitMQUtil;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 主程序入口，演示充电订单结算与积分增加的完整流程
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("====== 新能源车充电订单积分系统演示 ======");

        try {
            // 1. 初始化数据库
            DBUtil.initTables();

            // 2. 创建服务实例
            OrderSettlementService settlementService = new OrderSettlementService();
            PointsService pointsService = new PointsService();
            MessageProcessor messageProcessor = new MessageProcessor();
            messageProcessor.setCheckIntervalSeconds(10); // 每10秒检查一次
            messageProcessor.setMaxRetryCount(3); // 最大重试3次

            // 3. 创建并启动消息消费者
            PointsMessageConsumer pointsConsumer = new PointsMessageConsumer(pointsService);
            DeadLetterMessageConsumer dlqConsumer = new DeadLetterMessageConsumer(pointsService);
            pointsConsumer.startConsuming();
            dlqConsumer.startConsuming();

            // 4. 启动消息处理器
            messageProcessor.start();

            // 5. 准备测试数据
            String userId = "USER_" + System.currentTimeMillis();
            String orderId = "ORDER_" + UUID.randomUUID().toString().substring(0, 8);
            BigDecimal orderAmount = new BigDecimal("128.50"); // 订单金额

            System.out.println("\n====== 初始化状态 ======");
            System.out.println("用户ID: " + userId);
            System.out.println("初始积分: " + pointsService.getUserPoints(userId));

            // 6. 执行订单结算
            System.out.println("\n====== 执行订单结算 ======");
            System.out.println("订单ID: " + orderId);
            System.out.println("订单金额: " + orderAmount + "元");

            boolean settlementSuccess = settlementService.settleOrder(orderId, userId, orderAmount);
            if (!settlementSuccess) {
                System.out.println("订单结算失败，演示结束");
                return;
            }

            // 7. 等待积分处理完成
            System.out.println("\n====== 等待积分处理 ======");
            System.out.println("等待消息处理器和消费者处理积分...");

            // 等待30秒，观察积分到账过程
            for (int i = 0; i < 6; i++) {
                TimeUnit.SECONDS.sleep(5);
                System.out.println("已等待" + (i+1)*5 + "秒，当前积分: " + pointsService.getUserPoints(userId));
            }

            // 8. 展示最终结果
            System.out.println("\n====== 处理结果 ======");
            BigDecimal finalPoints = pointsService.getUserPoints(userId);
            System.out.println("用户最终积分: " + finalPoints);

            BigDecimal expectedPoints = orderAmount.multiply(new BigDecimal("0.01")).setScale(2);
            if (finalPoints.compareTo(expectedPoints) == 0) {
                System.out.println("积分正确到账，演示成功！");
            } else {
                System.out.println("积分未正确到账，可能需要人工干预");
            }

            // 9. 等待一段时间后停止服务
            TimeUnit.SECONDS.sleep(5);

        } catch (Exception e) {
            System.err.println("演示过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭资源
            System.out.println("\n====== 关闭服务 ======");
            RabbitMQUtil.close();
            System.out.println("====== 演示结束 ======");
            System.exit(0);
        }
    }
}
