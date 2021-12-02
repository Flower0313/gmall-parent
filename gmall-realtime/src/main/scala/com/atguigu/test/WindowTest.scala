package com.atguigu.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName gmall-parent-WindowTest 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日14:43 - 周四
 * @Describe
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    //需求：统计WordCount：3秒一个批次，窗口6秒
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("hadoop102", 31313)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    /*
    * 1.窗口的范围应该是采集周期的整数倍,因为不能采集到一半就结束啊
    * 2.窗口可以滑动，但在默认情况下，一个采集周期进行滑动(3)，那么这就会有重复数据，在中间的数据
    * 3.为避免重复数据，改变滑动步长,但有时又需要一部分重复数据，这就用reduceByKeyAndWindow
    * 4.如果有多批数据进入窗口,最终也会通过window操作变成统一的RDD处理
    * */

    //若你只填写一个参数，那么步长默认为一个采集周期
    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(3))

    windowDS.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
