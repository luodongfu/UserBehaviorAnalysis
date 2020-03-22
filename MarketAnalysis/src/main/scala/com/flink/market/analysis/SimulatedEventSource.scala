package com.flink.market.analysis

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.flink.market.model.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

//自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  // 定义是否运行的标志位
  var running = true
  // 定义用户行为的集合
  private val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  //定义渠道的集合
  private val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  //定义一个随机数发生器
  private val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个生成数据的上限
    val maxElements: Long = Long.MaxValue
    var count = 0L
    // 随机生成所有数据
    while (running && count < maxElements){
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channle: String = channelSets(rand.nextInt(channelSets.size))
      val ts: Long = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channle, ts))

      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}

