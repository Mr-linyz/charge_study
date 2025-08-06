package org.example.model;

import java.sql.Timestamp;

/**
 * 积分交易记录实体类
 */
public class PointsTransaction {
    private String transactionId;
    private String userId;
    private String orderId;
    private int points; // 正数为增加，负数为减少
    private String type; // CHARGING_REWARD: 充电奖励
    private String status; // PENDING, COMPLETED, FAILED
    private Timestamp createTime;
    private Timestamp completeTime;
    private String remark;

    // 构造函数、getter和setter
    public PointsTransaction() {}

    public PointsTransaction(String transactionId, String userId, String orderId, int points, String type) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.orderId = orderId;
        this.points = points;
        this.type = type;
        this.status = "PENDING";
        this.createTime = new Timestamp(System.currentTimeMillis());
    }

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public int getPoints() { return points; }
    public void setPoints(int points) { this.points = points; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Timestamp getCreateTime() { return createTime; }
    public void setCreateTime(Timestamp createTime) { this.createTime = createTime; }
    public Timestamp getCompleteTime() { return completeTime; }
    public void setCompleteTime(Timestamp completeTime) { this.completeTime = completeTime; }
    public String getRemark() { return remark; }
    public void setRemark(String remark) { this.remark = remark; }
}
    