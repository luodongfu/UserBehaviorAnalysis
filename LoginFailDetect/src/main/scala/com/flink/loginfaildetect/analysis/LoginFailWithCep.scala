package com.flink.loginfaildetect.analysis

import com.flink.loginfaildetect.model.LoginEvent
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.Map

/**
 * 恶意登录监控
 * @author yrp
 * @date 2020-03-20
 * @version 1.0
 */
object LoginFailWithCep {

  /**
   * CEP编程
   * LoginFail代码实现中我们可以看到，直接把每次登录失败的数据存起来、设置定时器一段时间后再读取，这种做法尽管简单，但和开始的需求还是略有差异的。
   * 这种做法只能隔2秒之后去判断一下这期间是否有多次失败登录，而不是在一次登录失败之后、再一次登录失败时就立刻报警。
   * 这个需求如果严格实现起来，相当于要判断任意紧邻的事件，是否符合某种模式。这听起来就很复杂了，那有什么方式可以方便地实现呢？
   * 很幸运，flink为我们提供了CEP（Complex Event Processing，复杂事件处理）库，用于在流中筛选符合某种复杂模式的事件。
   * 基于CEP来完成这个模块的实现。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
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
    // 定义匹配模式
    val loginPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.loginStatus == "fail")
      .next("next")
      .where(_.loginStatus == "fail")
      .within(Time.seconds(2))

    // 在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventSource.keyBy(_.userId), loginPattern)
    // .select方法传入一个 pattern select function，当检测到定义好的模式序列时就会调用
    val loginFailDataStream = patternStream.select((pattern:Map[String,Iterable[LoginEvent]])=>{
      val first = pattern.getOrElse("begin", null).iterator.next()
      val second = pattern.getOrElse("next", null).iterator.next()
      (second.userId, second.ip, second.loginStatus,second.eventTime)
    })

    //4. sink
    // 将匹配到的符合条件的事件打印出来
    loginFailDataStream.print()

    /**
     * (1,192.168.8.46,fail,1558430843)
     * (1,192.168.8.47,fail,1558430844)
     * (1,192.168.8.49,fail,1558430844)
     * (1,192.168.8.48,fail,1558430845)
     * (1,192.168.8.49,fail,1558430846)
     */
    env.execute("Login Fail Detect With Cep Job")
  }
}
