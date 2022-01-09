package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constans.GmallConstants
import com.atguigu.handler.DauHandler.{filterByRedis2, filterbyGroup, saveMidToRedis}
import com.atguigu.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ClassName gmall-parent-DeDuplication
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日18:37 - 周一
 * @Describe 需求一：统计日活，数据来源于脚本生成
 *
 *           数据来源:Kafka的主题=>GMALL_STARTUP
 *           数据去向:Hbase的表=>GMALL2021_DAU
 *
 */
object DeDuplication {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //背压机制
    //TODO 2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //TODO 3.连接kafka
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //TODO 4.将json数据转换为样例类
    //格式化时间
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    //mapPartitions是一次处理一个分区中的数据
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(x => {
      x.map(record => { //将一个分区中的元素单独进行处理
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val time: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = time.split(" ")(0)
        startUpLog.logHour = time.split(" ")(1)
        startUpLog
      })
    })
    startUpLogDStream.cache() //默认存储在内存

    /*
    * Q:为什么先批次间去重再批次内去重呢？
    * A:如果先执行批次内去重的话就有可能导致，这批数据其实都是重复数据，所以去重是没有意义的，
    *   如果先执行批次间去重的话，就可以得到去重后的数据一定都是新数据，没有重复数据
    * */

    //批次间去重,这里去重的是本批次数据和之前批次的数据
    val filterByRedisDStream: DStream[StartUpLog] = filterByRedis2(startUpLogDStream)
    //批次内去重，这里去重的本批次的数据
    val filterByGroupDStream: DStream[StartUpLog] = filterbyGroup(filterByRedisDStream)
    filterByGroupDStream.cache()

    //去重后存入redis
    saveMidToRedis(filterByGroupDStream)

    //将数据保存至Hbase,这里必须将hbase-site.xml文件放在resource中
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        //这里的字段名要和pheonix一样，要和传入的数据一一对应,传入的数据也是根据顺序填入的，所以顺序很重要
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create, //conf配置,这里调用的是resource中的hbase-site.xml
        Some("hadoop102,hadoop103,hadoop104:2181")) //集群
    })

    filterByGroupDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
