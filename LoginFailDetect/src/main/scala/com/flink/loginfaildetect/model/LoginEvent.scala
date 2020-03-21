package com.flink.loginfaildetect.model

/**
 * 输入的登录事件流
 * @param userId
 * @param ip
 * @param loginStatus
 * @param eventTime
 */
case class LoginEvent(userId: Long, ip: String, loginStatus: String, eventTime: Long)