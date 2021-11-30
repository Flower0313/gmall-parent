package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.bean.OrderInfo
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
 * @ClassName gmall-parent-GmvApp2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日22:28 - 周二
 * @Describe 只需要接收kafka中的数据，不用再启动CanalClient发送数据，直接将canal对接kafka
 */
object GmvApp2 {
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
        val info: JSONArray = JSON.parseObject(record.value()).getJSONArray("data")
        val orderInfo: OrderInfo = JSON.parseObject(info.get(0).toString, classOf[OrderInfo])
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
