package com.flink.hotItems.model

// 商品点击量(窗口操作的输出类型)
case class ItemViewCount (itemId: Long, windowEnd: Long, count: Long)

