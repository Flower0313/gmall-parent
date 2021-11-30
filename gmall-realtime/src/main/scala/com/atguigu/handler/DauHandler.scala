package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * @ClassName gmall-parent-DauHandler
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日20:29 - 周一
 * @Describe
 */
object DauHandler {
  //批次内去重
  def filterbyGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    val value: DStream[StartUpLog] = {
      val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.mapPartitions(partition => {
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })
      val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()
      val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })
      midAndDateToLogListDStream.flatMap(_._2)
    }
    value
  }

  //批次间去重：方案二
  def filterByRedis2(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth("w654646")

      val logs: Iterator[StartUpLog] = partition.filter(log => {
        val redisKey = "DAU:" + log.logDate
        val boolean: Boolean = jedisClient.sismember(redisKey, log.mid)
        !boolean
      })
      jedisClient.close()
      logs
    })
    value
  }

  //批次间去重：方案三
  def filterByRedis3(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    //方案三:在每个批次内创建一次连接，来优化连接个数
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.获取redis连接，在每个周期的driver端连接
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)

      //2.查redis中的mid
      //获取redisKey,这里取当前数据的原因就是实时数据的时间也是根据当前时间来的
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      //取出当天的数据
      val midS: util.Set[String] = jedisClient.smembers(redisKey)

      //3.将数据广播至executor端，然后将redis中的结果取出后发向Executor中内存，以供其中的task共用
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(midS)

      //4.根据获取到的mid去重，从这里开始就是在executor端执行了
      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })

      //关闭连接(driver端),这是关闭每个rdd的连接，反正数据已经取到了
      jedisClient.close()
      midFilterRDD
      /*
      * 总结:连接——取数据——关闭连接都是在driver端执行，然后将取到的数据广播给executor即可
      * */
    })
    value

  }

  //经过了上面的去重方法后就将数据添加到redis中，方便后面的去重
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //1.创建连接，在每个executor上执行
        val jedisClient: Jedis = new Jedis("hadoop102", 6379)
        jedisClient.auth("w654646")
        //2.在Executor上多个分区共用这个连接
        partition.foreach(log => {
          //每天都有不同的key，每个key存了今天的访问数据，若觉得占内存，可以flushDB昨天的数据
          val redisKey = "DAU:" + log.logDate
          //将mid存入redis,mid是唯一的,记住这里是按天创建key的
          jedisClient.sadd(redisKey, log.mid)
        })
        //关闭连接
        jedisClient.close()
      })
    })
  }
}