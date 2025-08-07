package com.charging.model;

import java.sql.Timestamp;

/**
 * 本地消息表实体类
 */
public class LocalMessage {
    private String messageId;
    private String businessType; // 业务类型：CHARGING_ORDER_SETTLEMENT
    private String businessId; // 业务ID：订单ID
    private String messageContent; // 消息内容，JSON格式
    private String status; // PENDING, SENT, CONFIRMED, FAILED
    private int retryCount; // 重试次数
    private Timestamp nextRetryTime; // 下次重试时间
    private Timestamp createTime;
    private Timestamp updateTime;

    // 构造函数、getter和setter
    public LocalMessage() {}

    public LocalMessage(String messageId, String businessType, String businessId, String messageContent) {
        this.messageId = messageId;
        this.businessType = businessType;
        this.businessId = businessId;
        this.messageContent = messageContent;
        this.status = "PENDING";
        this.retryCount = 0;
        this.createTime = new Timestamp(System.currentTimeMillis());
        this.updateTime = new Timestamp(System.currentTimeMillis());
        this.nextRetryTime = new Timestamp(System.currentTimeMillis()); // 立即重试
    }

    // Getters and Setters
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getBusinessType() { return businessType; }
    public void setBusinessType(String businessType) { this.businessType = businessType; }
    public String getBusinessId() { return businessId; }
    public void setBusinessId(String businessId) { this.businessId = businessId; }
    public String getMessageContent() { return messageContent; }
    public void setMessageContent(String messageContent) { this.messageContent = messageContent; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public Timestamp getNextRetryTime() { return nextRetryTime; }
    public void setNextRetryTime(Timestamp nextRetryTime) { this.nextRetryTime = nextRetryTime; }
    public Timestamp getCreateTime() { return createTime; }
    public void setCreateTime(Timestamp createTime) { this.createTime = createTime; }
    public Timestamp getUpdateTime() { return updateTime; }
    public void setUpdateTime(Timestamp updateTime) { this.updateTime = updateTime; }
}
    