package com.flink.ordertimeoutdetect.model

/**
 * 输出显示的订单状态结果
 * @param orderId
 * @param eventType
 */
case class OrderResult (orderId: Long, eventType: String)