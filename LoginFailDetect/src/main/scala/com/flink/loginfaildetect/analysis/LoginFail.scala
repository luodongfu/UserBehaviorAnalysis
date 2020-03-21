package com.flink.loginfaildetect.analysis

import com.flink.loginfaildetect.model.LoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 恶意登录监控
 * @author yrp
 * @date 2020-03-20
 * @version 1.0
 */
object LoginFail {

  /**
   * 状态编程
   * 由于同样引入了时间，最简单的方法其实与之前的热门统计类似，
   * 只需要按照用户ID分流，然后遇到登录失败的事件时将其保存在ListState中，
   * 然后设置一个定时器，2秒后触发。定时器触发时检查状态中的登录失败事件个数，如果大于等于2，那么就输出报警信息。
   * @param args
   */

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
    //由于没有现成的登录数据，我们用几条自定义的示例数据来做演示。
    import org.apache.flink.api.scala._
    val loginEventSource = env.fromCollection(List(LoginEvent(1, "192.168.8.45", "fail", 1558430842),
      LoginEvent(1, "192.168.8.46", "fail", 1558430843),
      LoginEvent(1, "192.168.8.47", "fail", 1558430844),
      LoginEvent(1, "192.168.8.48", "fail", 1558430845),
      LoginEvent(1, "192.168.8.49", "fail", 1558430846),
      LoginEvent(1, "192.168.8.49", "fail", 1558430848),
      LoginEvent(1, "192.168.8.49", "fail", 1558430844),
      LoginEvent(1, "192.168.8.49", "fail", 1558430852),
      LoginEvent(1, "192.168.8.49", "fail", 1558430856),
      LoginEvent(1, "192.168.8.49", "fail", 1558430858),
      LoginEvent(1, "192.168.8.50", "success", 1558430847)))
      .assignAscendingTimestamps(_.eventTime * 1000)

    //3. transformation
    val processed = loginEventSource
      .keyBy(_.userId)
      .process(new MatchFuction())

    //4. sink
    /**
     * 根据实际的需要，可以将Sink指定为Kafka、ES、Redis或其它存储
     */
    processed.print()

    /**
     * LoginEvent(1,192.168.8.49,fail,1558430858)
     */

    env.execute("Login Fail Detect Job")
  }

  class MatchFuction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
    // 定义状态变量
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved login", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
      if (value.loginStatus == "fail") {
        loginState.add(value)
      }
      // 注册定时器，触发事件设定为2秒后
      ctx.timerService().registerEventTimeTimer(value.eventTime + 2 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
      super.onTimer(timestamp, ctx, out)
      val allLogins: ListBuffer[LoginEvent] = ListBuffer()

      import scala.collection.JavaConversions._
      for (item <- loginState.get) {
        allLogins += item
      }
      loginState.clear()

      if (allLogins.length > 1) {
        out.collect(allLogins.last)
      }
    }
  }

}
