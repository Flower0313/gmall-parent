package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.StartUpLog
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Properties
import scala.util.Properties

/**
 * @ClassName gmall-parent-Test2 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日13:50 - 周二
 * @Describe
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //2.第二个参数是采集周期3秒，每3秒采集一次做统计分析,里面会创建SparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 31313)

    val value: DStream[StartUpLog] = lines.map(x => {
      val l: Array[String] = x.split(",")
      StartUpLog(l(0), l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8), l(9), l(10).toLong)
    })

    value.print()

    val properties: Properties = new Properties()
    properties.put("phoenix.schema.isNamespaceMappingEnabled", "true")

    value.foreachRDD(rdd => {
          rdd.saveToPhoenix(
            "GMALL2021_DAU",
            Seq("UID", "MID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
            HBaseConfiguration.create,
            Some("hadoop102,hadoop103,hadoop104:2181")) //集群
        })
    ssc.start()
    ssc.awaitTermination()
  }
}
