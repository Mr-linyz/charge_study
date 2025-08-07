package com.charging.scheduler;


import com.charging.manager.TccTransactionManager;
import com.charging.service.impl.ChargingService;
import com.charging.service.impl.PaymentService;
import com.charging.util.DBUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * 定时任务处理器 - 处理异常订单
 */
public class AbnormalOrderProcessor {
    private PaymentService paymentService;
    private ChargingService chargingService;
    private TccTransactionManager transactionManager;
    private long checkIntervalMinutes = 5; // 检查间隔(分钟)
    private long timeoutMinutes = 10; // 超时时间(分钟)

    public AbnormalOrderProcessor(PaymentService paymentService, ChargingService chargingService,
                                  TccTransactionManager transactionManager) {
        this.paymentService = paymentService;
        this.chargingService = chargingService;
        this.transactionManager = transactionManager;
    }

    /**
     * 启动定时任务
     */
    public void start() {
        Timer timer = new Timer("AbnormalOrderProcessor", true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                processAbnormalOrders();
            }
        }, 0, checkIntervalMinutes * 60 * 1000);

        System.out.println("异常订单处理定时任务已启动，检查间隔: " + checkIntervalMinutes + "分钟");
    }

    /**
     * 处理异常订单
     */
    private void processAbnormalOrders() {
        System.out.println("\n开始处理异常订单...");

        try (Connection conn = DBUtil.getConnection()) {
            // 获取异常的支付和充电订单ID
            List<String> abnormalPaymentTxIds = paymentService.getAbnormalPaymentTxIds(conn, timeoutMinutes);
            List<String> abnormalChargingTxIds = chargingService.getAbnormalChargingTxIds(conn, timeoutMinutes);

            // 合并并去重异常订单ID
            Set<String> abnormalTxIds = new HashSet<>();
            abnormalTxIds.addAll(abnormalPaymentTxIds);
            abnormalTxIds.addAll(abnormalChargingTxIds);

            System.out.println("发现 " + abnormalTxIds.size() + " 个异常订单需要处理");

            // 处理每个异常订单
            for (String txId : abnormalTxIds) {
                // 检查事务是否已经完成
                if (transactionManager.isTransactionCompleted(conn, txId)) {
                    System.out.println("事务已完成，跳过处理: " + txId);
                    continue;
                }

                try {
                    // 检查充电订单状态
                    Map<String, Object> order = chargingService.getOrder(conn, txId);
                    String orderStatus = order != null ? (String) order.get("status") : "UNKNOWN";

                    if ("FAILED".equals(orderStatus) || "CANCELED".equals(orderStatus)) {
                        // 充电失败或已取消，回滚支付
                        System.out.println("处理异常订单，执行回滚: " + txId);
                        transactionManager.rollback(conn, txId, paymentService, chargingService);
                    } else if ("IN_PROGRESS".equals(orderStatus)) {
                        // 充电中但超时，尝试确认
                        System.out.println("处理异常订单，尝试确认: " + txId);
                        transactionManager.commit(conn, txId, paymentService, chargingService);
                    } else {
                        // 其他未知状态，尝试回滚
                        System.out.println("处理异常订单，状态未知，执行回滚: " + txId);
                        transactionManager.rollback(conn, txId, paymentService, chargingService);
                    }
                } catch (Exception e) {
                    System.err.println("处理异常订单 " + txId + " 失败: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println("异常订单处理完成");
        } catch (SQLException e) {
            System.err.println("处理异常订单时发生数据库错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 设置检查间隔
    public void setCheckIntervalMinutes(long checkIntervalMinutes) {
        this.checkIntervalMinutes = checkIntervalMinutes;
    }

    // 设置超时时间
    public void setTimeoutMinutes(long timeoutMinutes) {
        this.timeoutMinutes = timeoutMinutes;
    }
}
    