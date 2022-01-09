package com.atguigu.utils

import com.atguigu.constans.GmallConstants
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 * @ClassName gmall-parent-OffsetManagerUtil 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年1月08日22:56 - 周六
 * @Describe 手动维护偏移量
 */
object OffsetManagerUtil {
  //从Redis中获取偏移量,type:hash , key: offset:topic:groupId , field:partition value:偏移量
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    //1.获取客户端连接
    val jedisClient: Jedis = new Jedis("hadoop102", 6379)
    jedisClient.auth("w654646")

    //2.拼接redis的key
    val offsetKey: String = "offset:" + topic + ":" + groupId

    //3.获取当前主题所有分区的偏移量(返回的是Java的Map集合)
    val offsetMap: util.Map[String, String] = jedisClient.hgetAll(offsetKey)

    //关闭连接
    jedisClient.close()

    //将java的Map转换为scala的
    import scala.collection.JavaConverters._
    val offMaps: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取偏移量:" + partition + ":" + offset)
        //将Map内部的元组进行转换
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap

    offMaps
  }

  def main(args: Array[String]): Unit = {
    println(getOffset(GmallConstants.KAFKA_TOPIC_STARTUP, "bigdata2021"))
  }


  //保存信息至Redis
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    println("保存偏移量")

    //拼接redis的key
    val offsetKey: String = "offset:" + topic + ":" + groupId

    //定义java的map集合,用于存放每个分区对应的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()


    //遍历
    for (elem <- offsetRanges) {
      //分区
      val partition: Int = elem.partition
      //结束偏移量
      val util: Long = elem.untilOffset
      offsetMap.put(partition.toString, util.toString)
      println("保存分区" + partition + ":" + elem.fromOffset + "->" + elem.untilOffset)
    }
    //获取客户端连接
    val jedisClient: Jedis = new Jedis("hadoop102", 6379)
    jedisClient.auth("w654646")
    if (!offsetMap.isEmpty) {
      jedisClient.hmset(offsetKey, offsetMap)
    }

    jedisClient.close()
  }
}
