package com.flink.market.model

// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)
