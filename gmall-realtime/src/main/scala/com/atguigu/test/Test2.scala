package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName gmall-parent-Test2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日13:50 - 周二
 * @Describe
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    val s1: Option[Int] = Some(5)
    val s2: Option[Int] = None

    println(s1.isDefined)
    println(s2.isDefined)
  }
}
