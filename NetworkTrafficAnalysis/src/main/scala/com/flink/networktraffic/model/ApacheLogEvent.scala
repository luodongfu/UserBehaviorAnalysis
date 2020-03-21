package com.flink.networktraffic.model

/**
 * 读取数据，并包装成ApacheLogEvent类型。
 * 输入日志流的数据格式
 * @param ip
 * @param userId
 * @param eventTime
 * @param method
 * @param url
 */
case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url: String )
