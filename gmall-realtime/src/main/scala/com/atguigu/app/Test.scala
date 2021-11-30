package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{OrderInfo, StartUpLog}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ClassName gmall-parent-jjjjjj 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日20:02 - 周一
 * @Describe
 */
object Test {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("T:/ShangGuiGu/gmall-parent/gmall-realtime/src/main/resources/order.txt")


    //格式化时间
    //val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val orderInfoDStream: RDD[OrderInfo] = rdd.mapPartitions(partition => {
      partition.map(record => {
        //有对应的名字的数据就装填进去，没有就为空
        val orderInfo: OrderInfo = JSON.parseObject(record, classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        orderInfo
      })
    })
    orderInfoDStream.collect().foreach(println)



    /*val rdd: RDD[StartUpLog] = sc.makeRDD(List(
      new StartUpLog("shanghai", "336", "andriod", "website", "gmall2019", "mid_236", "startup", "1.2.0", "2021-11-29", "19", "1638186135913".toLong),
      new StartUpLog("shandong", "368", "andriod", "wandoujia", "gmall2019", "mid_428", "startup", "1.1.2", "2021-11-29", "19", "1638186136624".toLong),
      new StartUpLog("beijing", "336", "andriod", "huawei", "gmall2019", "mid_395", "startup", "1.1.3", "2021-11-29", "19", "1638186136825".toLong),
      new StartUpLog("shan3xi", "346", "ios", "appstore", "gmall2019", "mid_50", "startup", "1.2.0", "2021-11-29", "19", "1638186137036".toLong),
      new StartUpLog("beijing", "41", "ios", "appstore", "gmall2019", "mid_748", "startup", "1.1.3", "2021-11-29", "19", "1638186137237".toLong)
    ))*/
    //先做去重能力强的，再做去重能力弱的
    /*val value1: RDD[StartUpLog] = filterbyGroup(filterByRedis2(value, sc))
    value1.collect().foreach(println)
    saveMidToRedis(value1)*/

    sc.stop()
  }

  //批次内去重
  def filterbyGroup(f1: RDD[StartUpLog]) = {
    val value: RDD[StartUpLog] = {
      //1.将数据转化为k，v((mid,logDate),log),mid是设备id，logDate是登陆日期
      val f2: RDD[((String, String), StartUpLog)] = f1.mapPartitions(partition => {
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })

      //2.groupByKey将相同key的数据聚和到同一个分区中,而(mid,logDate)作为key
      val f3: RDD[((String, String), Iterable[StartUpLog])] = f2.groupByKey()

      //3.将数据排序并取第一条数据,mapValues只对value进行操作
      val f4: RDD[((String, String), List[StartUpLog])] = f3.mapValues(iter => {
        //这里的iter就是指的List[StartUpLog],取集合中的比完大小的一个StartUpLog，按时间戳比大小,取每个用户第一次登陆的结果
        iter.toList.sortWith(_.ts < _.ts).take(1) //转换为List才能用sortWith
      })
      //4.将集合扁平化，因为List中存的是StartUpLog类，所以就将StartUpLog类依次取出即可
      f4.flatMap(_._2)
    }
    value
  }

  //批次间去重：方案一
  def filterByRedis1(startUpLogDStream: RDD[StartUpLog], sc: SparkContext) = {
    val value: RDD[StartUpLog] = startUpLogDStream.filter(log => {
      //创建redis连接，每个RDD进来都要创建连接，太冗余
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth("w654646")

      //设置redis的key，以DAU：登陆日期来做key
      val redisKey = "DAU:" + log.logDate

      //检查当前key是否是集合中的元素,若存在就返回true,不存在返回false
      val boolean: Boolean = jedisClient.sismember(redisKey, log.mid)

      //关闭连接
      jedisClient.close()
      //这里要将false转为true，因为算子是filter,所以存在的数据就不输出了，只有不存在才能输出
      !boolean
    })
    value
  }

  //批次间去重：方案二
  def filterByRedis2(startUpLogDStream: RDD[StartUpLog], sc: SparkContext) = {
    val value: RDD[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //创建redis连接,将连接写在分区中
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth("w654646")

      val logs: Iterator[StartUpLog] = partition.filter(log => {
        //redisKey
        val redisKey = "DAU:" + log.logDate

        //对比数据，重复的去掉，不重的留下来
        val boolean: Boolean = jedisClient.sismember(redisKey, log.mid)
        !boolean
      })
      //关闭连接
      jedisClient.close()
      logs
    })
    value
  }

  //批次间去重：方案三
  def filterByRedis3(startUpLogDStream: RDD[StartUpLog], sc: SparkContext) = {
    //方案三:在每个批次内创建一次连接，来优化连接个数
    /*val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.获取redis连接，在每个周期的driver端连接
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)

      //2.查redis中的mid
      //获取rediskey
      val rediskey = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

      val mids: util.Set[String] = jedisClient.smembers(rediskey)

      //3.将数据广播至executer端，然后将redis中的结果发向Executor中内存，以供其中的task共用
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.根据获取到的mid去重，从这里开始就是在executor端执行了
      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })

      //关闭连接
      jedisClient.close()
      midFilterRDD
    })
    value*/

  }

  //经过了上面的去重方法后就将数据添加到redis中，方便后面的去重
  def saveMidToRedis(startUpLogDStream: RDD[StartUpLog]) = {
    startUpLogDStream.foreachPartition(partition => {
      //1.创建连接，在每个executor上执行
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth("w654646")

      //2.在Executor上多个分区共用这个连接
      partition.foreach(log => {
        //redisKey
        val redisKey = "DAU:" + log.logDate
        //将mid存入redis
        jedisClient.sadd(redisKey, log.mid)
      })
      //关闭连接
      jedisClient.close()
    })

  }

}

