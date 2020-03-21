package com.flink.ordertimeoutdetect.model

/**
 * 是输入的订单事件流；
 * @param orderId
 * @param eventType
 * @param eventTime
 */
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

