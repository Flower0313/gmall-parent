package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @ClassName gmall-parent-UserInfoApp 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日20:30 - 周五
 * @Describe 采集userInfo进入缓存
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val userDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    val infoJson: DStream[String] = userDStream.map(record => {
      record.value()
    })

    infoJson.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //a.获取连接,在executor执行,每个分区执行一次
        val jedisClient: Jedis = new Jedis("hadoop102", 6379)
        jedisClient.auth(GmallConstants.PASSWORD)
        //b.写库
        partition.foreach(info => {
          val userInfo: UserInfo = JSON.parseObject(info, classOf[UserInfo])
          val userInfoKey: String = "userInfo" + userInfo.id
          //id是userInfo的主键,那这里肯定唯一,用set即可
          jedisClient.set(userInfoKey, info)
        })
        //c.关闭连接
        jedisClient.close()
      })
    })
    infoJson.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
