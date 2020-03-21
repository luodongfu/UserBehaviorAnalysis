package com.flink.hotItems.model

case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timeStamp: Long)
