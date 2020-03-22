package com.flink.networktraffic.analysis

import java.net.URL

import com.flink.networktraffic.model.NetWorkFlowBean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 统计PV
 */
object PageView {
  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
    //用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[(String, Int)] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //升序数据 * 1000 表示秒
      //3. transformation
      .filter(_.behavior == "pv") //只统计pv操作
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) //窗口大小是一个小时, 1小时输出一次
      .sum(1)

    //4. sink
    dataStream.print("pv count ==> ")

    /**
     * pv count ==> > (pv,3)
     * pv count ==> > (pv,1)
     */

    env.execute("Page View Job")
  }
}
