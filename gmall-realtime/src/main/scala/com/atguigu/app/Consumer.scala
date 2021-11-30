package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat


/**
 * @ClassName gmall-parent-DauApp 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日15:58 - 周一
 * @Describe 消费Kafka
 */
object Consumer {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Consumer")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))//5秒一次

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.
      getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    kafkaDStream.foreachRDD(rdd => { //遍历DStream中的RDD
      rdd.foreach(record => { //遍历RDD中的元素
        println(record.value())
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
