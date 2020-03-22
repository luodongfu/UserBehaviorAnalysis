package com.flink.market.model

//输出黑名单报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)