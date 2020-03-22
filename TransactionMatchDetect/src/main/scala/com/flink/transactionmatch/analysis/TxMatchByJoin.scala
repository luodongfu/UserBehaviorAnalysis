package com.flink.transactionmatch.analysis

import java.net.URL

import com.flink.transactionmatch.model.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{KeyedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 使用join做对账
 */
object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
    // 读取订单事件流
    val resource: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptResource: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStrem: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)// 两条流的watermark，以慢的那条流的时间为watermark的时间
      .keyBy(_.txId)

    //3. transformation
    // 做join处理
    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.intervalJoin(receiptEventStrem)
      .between(Time.seconds(-5), Time.seconds(5))// 时间间隔
      .process(new TxPayMatchByJoin())

    //4. sink
    processedStream.print()

    /**
     * (OrderEvent(34729,pay,sd76f87d6,1558430844),ReceiptEvent(sd76f87d6,wechat,1558430847))
     * (OrderEvent(34730,pay,3hu3k2432,1558430845),ReceiptEvent(3hu3k2432,alipay,1558430848))
     */

    env.execute("Transaction Match By Join Job")
  }
}

class TxPayMatchByJoin extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
