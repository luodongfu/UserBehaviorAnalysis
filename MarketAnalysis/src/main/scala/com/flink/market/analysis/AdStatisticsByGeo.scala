package com.flink.market.analysis

import java.net.URL
import java.sql.Timestamp

import com.flink.market.model.{AdClickEvent, BlackListWarning, CountByProvince}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 根据地理位置统计广告信息
 * 过滤掉刷单行为的用户
 * 输出黑名单报警信息
 */
object AdStatisticsByGeo {
  // 定义侧输出流的标签
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
    val resourceUrl: URL = getClass.getResource("/AdClickLog.csv")
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile(resourceUrl.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //3. transformation
    // 自定义process function, 过滤大量刷点击的行为
    val filterBlackListStream = adEventStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))

    //根据省份做分组，开窗，聚合
    val adCountStream: DataStream[CountByProvince] = adEventStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    //4. sink
    adCountStream.print("===> ")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList ===>　")


    env.execute("Market Ad Analysis Job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
    // 定义状态， 保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出count状态
      val curCount: Long = countState.value()

      // 如果是第一次处理， 注册定时器, 每天00:00触发
      if (curCount == 0){
        val ts: Long = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断技术是否达到上限， 如果到达则加入黑名单
      if (curCount >= maxCount){
        // 判断是否发送过黑名单， 只发送一次
        if (isSentBlackList.value()) {
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over "+ maxCount + " time today."))
        }
        return
      }
      // 计数状态加1， 输出数据到主流
      countState.update(curCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()){
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }
}

class AdCountAgg extends AggregateFunction[AdClickEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}
