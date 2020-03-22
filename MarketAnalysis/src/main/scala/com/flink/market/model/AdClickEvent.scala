package com.flink.market.model

// 输入的广告点击时间样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
