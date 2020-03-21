package com.flink.networktraffic.model

// 窗口操作统计的输出数据类型格式
case class UrlViewCount( url: String, windowEnd: Long, count: Long )
