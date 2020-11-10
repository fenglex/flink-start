package com.fenglex.flink.simple

/**
 * @author haifeng
 * @version 1.0
 * @date 2020/11/10 14:16
 */

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "D:\\a.txt"

    val inputDS: DataSet[String] = env.readTextFile(inputPath)

    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map(x => x + "123").map((_, 1)).groupBy(0).sum(1)

    wordCountDS.print()
    print("-----------------------------------")
    val wordCountDS2: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map(x => x + "123").map((_, 1)).reduce((x, y) => {
      (x._1, x._2 + y._2)
    }).groupBy(0).sum(1)

    wordCountDS2.print()
  }
}