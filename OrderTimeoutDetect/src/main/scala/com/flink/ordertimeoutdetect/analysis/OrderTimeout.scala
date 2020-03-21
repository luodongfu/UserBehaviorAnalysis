package com.flink.ordertimeoutdetect.analysis

import java.util
import com.flink.ordertimeoutdetect.model.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 订单支付实时监控
 * @author yrp
 * @date 2020-03-20
 * @version 1.0
 */

/**
 * 实际需求
 * 在电商平台中，最终创造收入和利润的是用户下单购买的环节；更具体一点，是用户真正完成支付动作的时候。
 * 用户下单的行为可以表明用户对商品的需求，但在现实中，并不是每次下单都会被用户立刻支付。当拖延一段时间后，
 * 用户支付的意愿会降低。所以为了让用户更有紧迫感从而提高支付转化率，同时也为了防范订单支付环节的安全风险，
 * 电商网站往往会对订单状态进行监控，设置一个失效时间（比如15分钟），如果下单后一段时间仍未支付，订单就会被取消。
 */
object OrderTimeout {

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. read data
    // 由于没有现成的数据，我们还是用几条自定义的示例数据来做演示。
    import org.apache.flink.api.scala._
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    /**
     * 将会利用CEP库来实现这个功能。先将事件流按照订单号orderId分流，
     * 然后定义这样的一个事件模式：在15分钟内，事件“create”与“pay”严格紧邻：
     */
    //3. transformation
    // 定义一个带匹配时间窗口的模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .next("next")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")
    // 订单事件流根据 orderId 分流，然后在每一条流中匹配出定义好的模式
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    import scala.collection.Map

    // 这样调用.select方法时，就可以同时获取到匹配出的事件和超时未匹配的事件了。
//    val complexResult = patternStream.select(orderTimeoutOutput) {
//      // 对于已超时的部分模式匹配的事件序列，会调用这个函数
//      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
//        val createOrder = pattern.get("begin")
//        OrderResult(createOrder.get.iterator.next().orderId, "timeout")
//      }
//    } {
//      // 检测到定义好的模式序列时，就会调用这个函数
//      pattern: Map[String, Iterable[OrderEvent]] => {
//        val payOrder = pattern.get("next")
//        OrderResult(payOrder.get.iterator.next().orderId, "success")
//      }
//    }
    val complexResult = patternStream.select(orderTimeoutOutput,new OrderTimeoutSelect,new OrderPaySelect)

    // 拿到同一输出标签中的 timeout 匹配结果（流）
    val timeoutResult = complexResult.getSideOutput(orderTimeoutOutput)

    //4. sink
    complexResult.print()
    timeoutResult.print()

    /**
     * OrderResult(1,timeout)
     * OrderResult(2,success)
     */

    env.execute("Order Timeout Detect Job")

  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  // l是超时的时间戳
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    // 此时get的可以begin或follow
    val payedOrderId = map.get("next").iterator().next().orderId
    OrderResult(payedOrderId, "success")
  }
}
