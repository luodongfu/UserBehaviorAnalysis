package com.flink.networktraffic.analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.flink.networktraffic.model.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实时流量统计
 * 分析apache服务器的日志文件apache.log
 * @version: 1.0
 * @author yrp
 * @date 2020-03-20
 */
object NetworkTraffic {

  def main(args: Array[String]): Unit = {

    // 1. create environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
    val networkTrafficSource = env.readTextFile("/Users/yinruipeng/codes/UserBehaviorAnalysis/NetworkTrafficAnalysis/src/main/resources/apache.log")
      .map(line =>{
        val linearray = line.split(" ")
        // 定义时间转换模板将时间转成时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        ApacheLogEvent( linearray(0), linearray(1), timestamp, linearray(5), linearray(6) )
      })
      // 乱序数据处理，创建时间戳和水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })

    //3. transformation
    val processed = networkTrafficSource
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate( new CountAgg(), new WindowResultFunction() )
      .keyBy(_.windowEnd)
      .process( new TopNHotUrls(5) )

    //4. sink
    processed.print()

    /**
     * ====================================
     * 时间: 2015-05-17 10:05:15.0
     * No1:  URL=/reset.css  流量=3
     * No2:  URL=/blog/tags/puppet?flav=rss20  流量=2
     * No3:  URL=/style2.css  流量=2
     * No4:  URL=/presentations/logstash-monitorama-2013/plugin/notes/notes.js  流量=1
     * No5:  URL=/presentations/logstash-monitorama-2013/images/kibana-dashboard2.png  流量=1
     * ====================================
     *
     *
     * ====================================
     * 时间: 2015-05-17 10:05:20.0
     * No1:  URL=/reset.css  流量=3
     * No2:  URL=/blog/tags/puppet?flav=rss20  流量=2
     * No3:  URL=/style2.css  流量=2
     * No4:  URL=/blog/tags/munin  流量=1
     * No5:  URL=/presentations/logstash-monitorama-2013/plugin/notes/notes.js  流量=1
     * ====================================
     */

    env.execute("Network Traffic Analysis Job")
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long]{
    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def createAccumulator(): Long = 0L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key
      val count = input.iterator.next()
      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  // 自定义process function，统计访问量最大的url，排序输出
  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

    // 直接定义状态变量，懒加载
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState( new ListStateDescriptor[UrlViewCount]( "urlState", classOf[UrlViewCount] ) )

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 把每条数据保存到状态中
      urlState.add(i)
      // 注册一个定时器，windowEnd + 10秒 时触发
      context.timerService().registerEventTimeTimer(i.windowEnd + 10 * 1000)
    }

    // 实现ontimer
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 从状态中获取所有的Url访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for( urlView <- urlState.get() ){
        allUrlViews += urlView
      }
      // 清空state
      urlState.clear()
      // 按照访问量从大到小排序排序输出
      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 10 * 1000)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentUrlView.url)
          .append("  流量=").append(currentUrlView.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果

      Thread.sleep(1000)
      out.collect(result.toString())

    }
  }
}
