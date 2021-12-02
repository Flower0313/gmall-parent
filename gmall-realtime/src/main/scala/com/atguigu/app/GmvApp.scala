package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName gmall-parent-GmvApp 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日20:04 - 周二
 * @Describe 需求二：交易额，需要开启CanalClient来像kafka传参，数据来源于mysql
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //todo 1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    //todo 2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //todo 3.获取kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //todo 4.将JSON数据转换为样例类
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
      //一个DStream由多个RDD组成，一个RDD里可以有多个分区,一个分区对应一个task,一个分区中有多个元素
      partition.map(record => {
        //有对应的名字的数据就装填进去，没有就为空
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //create_time的数据类似这样:2020-06-20 16:12:39
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        orderInfo
      })
    })

    //todo 5.将数据写入Hbase
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL2021_ORDER_INFO",
        //数据按顺序写入，所以写入前就要按照这样排好序
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    orderInfoDStream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
