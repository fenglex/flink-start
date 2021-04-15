package com.fenglex

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/4/13 15:56
 */
object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    val alertStream = dataStream
      .keyBy(_.id)
      //      .flatMap( new TempChangeAlert(10.0) )
      .map(new MapWithStates(10))
      .filter(_._1.nonEmpty)
    //      .flatMapWithState[(String, Double, Double), Double] {
    //        case (data: SensorReading, None) =>
    //          println(998)
    //          (List.empty, Some(data.temperature))
    //        case (data: SensorReading, lastTemp: Some[Double]) =>
    //          // 跟最新的温度值求差值作比较
    //          val diff = (data.temperature - lastTemp.get).abs
    //          if (diff > 10.0) {
    //            println(123)
    //            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
    //          } else {
    //            println(1)
    //            (List.empty, Some(data.temperature))
    //          }
    //      }

    alertStream.print()

    env.execute("state test")
  }
}

class MapWithStates(up: Int) extends RichMapFunction[SensorReading, (String, Double, Double)] {
  lazy val lastState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastValue", classOf[Double]))
  private var first: Boolean = true

  override def map(in: SensorReading): (String, Double, Double) = {
    if (first) {
      first = false;
      lastState.update(in.temperature)
      ("", 1, 1)
    } else {
      val lastValue = lastState.value()
      val inc = (lastValue - in.temperature).abs
      lastState.update(in.temperature)
      if (inc > up) {
        (in.id, lastValue, in.temperature)
      } else {
        ("", 1, 1)
      }

    }
  }
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)