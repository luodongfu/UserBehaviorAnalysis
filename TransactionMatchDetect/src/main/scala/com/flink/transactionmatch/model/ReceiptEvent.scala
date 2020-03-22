package com.flink.transactionmatch.model

// 接收流事件样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)
