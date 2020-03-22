package com.flink.networktraffic.model

import scala.beans.BeanProperty

object NetWorkFlowBean {
  @BeanProperty
  case class ApacheLogEvent(var ip: String, var key: String, var eventTime: Long, var categoryId:String, var url: String)

  @BeanProperty
  case class UrlViewCount(var url: String, var windowEnd: Long, var count: Long)

  @BeanProperty
  case class UserBehavior(userId: Long, itemId: Long,  categoryId: Int, behavior: String, timestamp: Long)

  @BeanProperty
  case class UvCount(windowEnd: Long, uvCount: Long)
}
