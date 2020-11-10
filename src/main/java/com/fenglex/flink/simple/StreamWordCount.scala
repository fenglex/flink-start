package com.fenglex.flink.simple

/**
 * @author haifeng
 * @version 1.0
 * @date 2020/11/10 14:21
 */

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textDStream: DataStream[String] = env.socketTextStream(host, port)


    val dataStream: DataStream[(String, Int)] =
      textDStream.flatMap(_.split("\\s")).map((_, 1)).keyBy(_._1).sum(1)

    dataStream.print().setParallelism(1)

    env.execute("Socket stream word count")
  }
}