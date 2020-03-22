package com.flink.market.analysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.flink.market.model.{MarketingUserBehavior, MarketingViewCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * APP 不同市场渠道用户量统计
 */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp) //使用数据中时间升序作为时间时间
      .filter(_.behavior != "UNINSTALL") // 过滤到卸载用户
      .map(data => {
        ((data.channel, data.behavior), 1L)
      }) // 包装成元祖， 后面做聚合统计
      .keyBy(_._1) // 将二元组（渠道和行为类型）相同的数据分组/分类到一起
      .timeWindow(Time.hours(1), Time.seconds(10)) // 统计一小时之内的， 每10秒做一次统计
      .process(new MarketingCountByChannel())

    dataStream.print("=======>  ")

    /**
     * =======>  > MarketingViewCount(2020-03-22 09:17:50.0,2020-03-22 10:17:50.0,huaweistore,CLICK,47)
     * =======>  > MarketingViewCount(2020-03-22 09:18:00.0,2020-03-22 10:18:00.0,appstore,DOWNLOAD,88)
     *
     **/

    env.execute("App Marketing By Channel Job")
  }
}
// 自定义处理函数
class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs: String = new Timestamp(context.window.getStart).toString
    val endTs: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2
    val count: Int = elements.size
    out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}