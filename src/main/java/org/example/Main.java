package org.example;

import org.example.manager.TccTransactionManager;
import org.example.scheduler.AbnormalOrderProcessor;
import org.example.scheduler.MessageProcessor;
import org.example.service.impl.ExtendedChargingService;
import org.example.service.impl.PaymentService;
import org.example.service.impl.PointsService;
import org.example.util.DBUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 演示充电订单结算和积分增加流程
 */
public class Main {
    public static void main(String[] args) {
        // 初始化数据库表
        DBUtil.initTables();

        // 初始化服务
        PaymentService paymentService = new PaymentService();
        ExtendedChargingService chargingService = new ExtendedChargingService();
        PointsService pointsService = new PointsService();
        TccTransactionManager transactionManager = new TccTransactionManager();

        // 启动异常订单处理定时任务
        AbnormalOrderProcessor orderProcessor = new AbnormalOrderProcessor(paymentService, chargingService, transactionManager);
        orderProcessor.start();

        // 启动消息处理器
        MessageProcessor messageProcessor = new MessageProcessor(pointsService);
        messageProcessor.setCheckIntervalSeconds(30); // 每30秒检查一次
        messageProcessor.start();

        // 测试数据
        String userId = "user1";
        String chargingPointId = "CP12345";
        double chargeAmount = 100.0;
        String orderId = null;

        try (Connection conn = DBUtil.getConnection()) {
            // 1. 开始事务并完成充电
            String txId = transactionManager.beginTransaction(conn);

            try {
                // 执行支付的Try操作
                boolean paymentTryResult = transactionManager.executeTry(conn, txId, paymentService, userId, chargeAmount);

                // 执行充电的Try操作
                boolean chargingTryResult = transactionManager.executeTry(conn, txId, chargingService, chargingPointId, userId, chargeAmount);

                // 如果所有Try操作都成功，则提交事务
                if (paymentTryResult && chargingTryResult) {
                    boolean commitResult = transactionManager.commit(conn, txId, paymentService, chargingService);
                    if (commitResult) {
                        System.out.println("充电交易成功完成");

                        // 获取订单ID
                        Map<String, Object> order = chargingService.getOrder(conn, txId, null);
                        if (order != null) {
                            orderId = (String) order.get("order_id");
                            System.out.println("充电订单ID: " + orderId);
                        }
                    } else {
                        throw new RuntimeException("提交事务失败");
                    }
                } else {
                    throw new RuntimeException("Try阶段有操作失败");
                }
            } catch (Exception e) {
                System.out.println("异常: " + e.getMessage());
                // 回滚事务
                try {
                    transactionManager.rollback(conn, txId, paymentService, chargingService);
                    System.out.println("交易已取消，已触发自动退款");
                } catch (SQLException rollbackEx) {
                    System.err.println("回滚操作失败，将由定时任务处理: " + rollbackEx.getMessage());
                }
            }

            // 2. 模拟运营商推送订单后，回调过来进行结算
            if (orderId != null) {
                System.out.println("\n开始结算订单: " + orderId);
                boolean settleSuccess = chargingService.settleChargingOrder(orderId);

                if (settleSuccess) {
                    System.out.println("订单结算成功，等待积分到账");
                } else {
                    System.out.println("订单结算失败");
                }
            }

            // 3. 显示当前状态
            System.out.println("\n当前状态:");
            System.out.println("用户余额: " + paymentService.getUserBalance(conn, userId));
            System.out.println("用户当前积分: " + pointsService.getUserPoints(conn, userId));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 保持程序运行，让定时任务有机会执行
        try {
            System.out.println("\n等待消息处理器处理积分消息...");
            TimeUnit.MINUTES.sleep(2); // 等待2分钟

            // 再次查询积分
            try (Connection conn = DBUtil.getConnection()) {
                System.out.println("\n最终状态:");
                System.out.println("用户最终积分: " + pointsService.getUserPoints(conn, userId));
            }
        } catch (InterruptedException | SQLException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 停止定时任务
            messageProcessor.stop();
        }
    }
}
