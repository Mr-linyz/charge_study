package com.charging;

import com.charging.scheduler.MessageProcessor;
import com.charging.service.OrderSettlementService;
import com.charging.service.PointsService;
import com.charging.util.DBUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 主程序演示类，展示充电订单结算与积分增加流程
 */
public class Main {

    public static void demo() {

        System.out.println("====== 充电订单与积分一致性流程演示 ======");

        // 初始化数据库表结构
        initDatabase();

        // 创建Spring上下文
        ApplicationContext context = new AnnotationConfigApplicationContext("com.charging");

        // 获取服务实例
        OrderSettlementService settlementService = context.getBean(OrderSettlementService.class);
        PointsService pointsService = context.getBean(PointsService.class);
        MessageProcessor messageProcessor = context.getBean(MessageProcessor.class);

        try {
            // 准备测试数据
            String userId = "USER_" + System.currentTimeMillis();
            String orderId = "ORDER_" + UUID.randomUUID().toString().substring(0, 8);
            double orderAmount = 156.80; // 订单金额

            System.out.println("\n[1] 初始状态");
            System.out.println("用户ID: " + userId);
            System.out.println("初始积分: " + pointsService.getUserPoints(userId));

            // 订单结算
            System.out.println("\n[2] 开始订单结算");
            System.out.println("订单ID: " + orderId);
            System.out.println("订单金额: " + orderAmount + "元");
            boolean settlementSuccess = settlementService.settleOrder(orderId, userId, orderAmount);

            if (settlementSuccess) {
                System.out.println("订单结算成功，已创建积分消息");
                double expectedPoints = orderAmount * 0.01; // 1%比例转换为积分
                System.out.println("预计获得积分: " + expectedPoints);
            } else {
                System.out.println("订单结算失败，流程终止");
                return;
            }

            // 启动消息处理器
            System.out.println("\n[3] 启动消息处理器");
            messageProcessor.start();

            // 等待积分处理
            System.out.println("\n[4] 等待积分处理（30秒）");
            for (int i = 0; i < 6; i++) {
                TimeUnit.SECONDS.sleep(5);
                System.out.println("已等待 " + (i + 1) * 5 + " 秒，当前积分: " + pointsService.getUserPoints(userId));
            }

            // 展示最终结果
            System.out.println("\n[5] 处理结果");
            double finalPoints = pointsService.getUserPoints(userId);
            System.out.println("用户最终积分: " + finalPoints);
            System.out.println("积分明细: " + pointsService.getTransactionDetails(userId));

        } catch (Exception e) {
            System.err.println("演示过程出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            messageProcessor.stop();
            DBUtil.close();
            System.out.println("\n====== 演示结束 ======");
        }
    }

    /**
     * 初始化数据库表结构
     */
    private static void initDatabase() {
        System.out.println("初始化数据库表结构...");
        DBUtil.initTables();
        System.out.println("数据库初始化完成");
    }
}
