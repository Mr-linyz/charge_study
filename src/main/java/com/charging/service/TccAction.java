package com.charging.service;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * TCC模式接口定义
 */
public interface TccAction {
    /**
     * 尝试执行
     */
    boolean tryAction(Connection conn, String txId, Object... args) throws SQLException;
    
    /**
     * 确认执行
     */
    boolean confirmAction(Connection conn, String txId) throws SQLException;
    
    /**
     * 取消执行
     */
    boolean cancelAction(Connection conn, String txId) throws SQLException;
}
    