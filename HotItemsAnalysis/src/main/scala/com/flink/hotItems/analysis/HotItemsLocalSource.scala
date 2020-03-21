package com.flink.hotItems.analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.util.Properties

import com.flink.hotItems.model.{ItemViewCount, UserBehavior}

import scala.collection.mutable.ListBuffer

/**
 * 实时热门商品统计
 * @author yrp
 * @date 2020-03-21
 * @version 1.0
 */
object HotItemsLocalSource {
  def main(args: Array[String]): Unit = {

    // 1. create environment
    // 创建一个 StreamExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定Time类型为EventTime  按照业务时间处理 Flink默认使用ProcessingTime处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
    env.setParallelism(1)


    // 2. read data
    import org.apache.flink.api.scala._

    //662867,2244074,1575622,pv,1511658000
    // 以window下为例，需替换成自己的路径
    val localDataSource = env.readTextFile("/Users/yinruipeng/codes/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        if (linearray.size >= 5) {
          UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toLong, linearray(3), linearray(4).toLong)
        } else {
          UserBehavior(0l, 0l, 0l, "", 0l)
        }
      }).assignAscendingTimestamps(_.timeStamp * 1000)  // 指定时间戳和watermark  // 转化为ms

      //3. transformation
      val processed = localDataSource
        .filter(_.behavior.equals("pv"))  //过滤出点击事件
        .keyBy("itemId")  //对商品进行分组
        .timeWindow(Time.minutes(60), Time.minutes(5))
        .aggregate(new CountAgg, new WindowResultFunction)
        .keyBy("windowEnd")
        .process(new TopNHotItems(3))


    //4. sink
    processed.print()

    /**
     * ====================================
     * 时间: 2017-11-26 09:15:00.0
     * No1:  商品ID=812879  浏览量=7
     * No2:  商品ID=138964  浏览量=5
     * No3:  商品ID=4568476  浏览量=5
     * ====================================
     *
     * ====================================
     * 时间: 2017-11-26 09:20:00.0
     * No1:  商品ID=812879  浏览量=8
     * No2:  商品ID=2338453  浏览量=8
     * No3:  商品ID=2563440  浏览量=7
     * ====================================
     */

    /**
     * assignAscendingTimestamps(_.timeStamp * 1000)第二件事情是指定如何获得业务时间，以及生成Watermark。
     * Watermark是用来追踪业务事件的概念，可以理解成EventTime世界中的时钟，用来指示当前处理到什么时刻的数据了。
     * 由于我们的数据源的数据已经经过整理，没有乱序，即事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当做Watermark。
     * 这里我们用 assignAscendingTimestamps来实现时间戳的抽取和Watermark的生成。
     * 这样我们就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作。
     *
     * 注：真实业务场景一般都是乱序的，所以一般不用assignAscendingTimestamps，而是使用BoundedOutOfOrdernessTimestampExtractor。
     */

    /**
     * 设置滑动窗口，统计点击量
     * 由于要每隔5分钟统计一次最近一小时每个商品的点击量，所以窗口大小是一小时，每隔5分钟滑动一次。即分别要统计[09:00, 10:00), [09:05, 10:05),
     * [09:10, 10:10)…等窗口的商品点击量。是一个常见的滑动窗口需求（Sliding Window）。
     * .timeWindow(Time.minutes(60), Time.minutes(5))
     */

    /**
     * 使用 .aggregate(AggregateFunction af, WindowFunction wf) 做增量的聚合操作，
     * 它能使用AggregateFunction提前聚合掉数据，减少state的存储压力。
     * 较之 .apply(WindowFunction wf) 会将窗口中的数据都存储下来，最后一起计算要高效地多。
     * 这里的CountAgg实现了AggregateFunction接口，功能是统计窗口中的条数，即遇到一条数据就加一。
     */

    /**
     * 为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组，这里根据ItemViewCount中的windowEnd进行keyBy()操作。
     * 然后使用ProcessFunction实现一个自定义的TopN函数TopNHotItems来计算点击量排名前3名的商品，并将排名结果格式化成字符串，便于后续输出。
     * .keyBy("windowEnd")
     * .process(new TopNHotItems(3));  // 求点击量前3名的商品
     */
    env.execute("Hot Items Analysis Job")
  }

  // COUNT统计的聚合函数实现，每出现一条记录就加一
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  /**
   * 用于输出窗口的结果:
   * 聚合操作.aggregate(AggregateFunction af, WindowFunction wf)的第二个参数WindowFunction将每个key每个窗口聚合后的结果带上其他信息进行输出。
   * 我们这里实现的WindowResultFunction将<主键商品ID，窗口，点击量>封装成了ItemViewCount进行输出。
   */
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }
  //现在得到了每个商品在每个窗口的点击量的数据流。

  /**
   * 计算最热门Top N商品
   * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
   * 然后使用ProcessFunction实现一个自定义的TopN函数TopNHotItems来计算点击量排名前3名的商品，并将排名结果格式化成字符串，便于后续输出。
   * ProcessFunction是Flink提供的一个low-level API，用于实现更高级的功能。
   * 它主要提供了定时器timer的功能（支持EventTime或ProcessingTime）。
   * 本案例中我们将利用timer来判断何时收齐了某个window下所有商品的点击量数据。
   * 由于Watermark的进度是全局的，在processElement方法中，每当收到一条数据ItemViewCount，
   * 我们就注册一个windowEnd+1的定时器（Flink框架会自动忽略同一时间的重复注册）。
   * windowEnd+1的定时器被触发时，意味着收到了windowEnd+1的Watermark，即收齐了该windowEnd下的所有商品窗口统计值。
   * 我们在onTimer()中处理将收集的所有商品及点击量进行排序，选出TopN，并将排名信息格式化成字符串后进行输出。
   * 这里我们还使用了ListState<ItemViewCount>来存储收到的每条ItemViewCount消息，保证在发生故障时，状态数据的不丢失和一致性。
   * ListState是Flink提供的类似Java List接口的State API，它集成了框架的checkpoint机制，自动做到了exactly-once的语义保证。
   *
   * @param topSize
   */
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var listState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
      // 定义状态变量
      listState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      listState.add(value)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      // 获取收到的所有商品点击量
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._

      for (item <- listState.get()) {
        allItems += item
      }
      listState.clear()

      // 按照点击量从大到小排序
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedItems.indices) {
        val currentItem: ItemViewCount = sortedItems(i)
        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i + 1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }


}