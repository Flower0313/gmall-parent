package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter}

/**
 * @ClassName gmall-parent-SaleDetailApp 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日16:04 - 周五
 * @Describe 灵活分析需求:开启顺序CanalClient => UserInfoApp => SaleDetailApp
 *
 *           Q1:为什么需要将orderInfo和orderDetail存入缓存呢?
 *           A1:因为传输的过程中有延迟,在kafka传输过程中有可能有些数据慢了，导致没有join上，所以没join上的数据都会
 *           存一份在redis,这样不管谁先谁后，后面的那个进来时都可以判断前面是否有遗留没关联上的数据,再设置一个过期时间
 *           超过这个过期时间的话就会删除redis中的数据，一般达到了过期时间的都证明这条数据不存在关联对象。
 *
 *           Q2:为什么orderInfo每份都要缓存在redis呢?
 *           A2:因为orderInfo:orderDetail = 1:n,所以后面可能还会有相应的orderDetail过来,所以一定要都备份一遍,
 *           而orderDetail只会对应一个orderInfo,取出来关联上就行了,所以只存没有关联上的orderDetail就行。
 *           orderInfo使用set存,orderDetail使用sadd存,我们使用的都是order_id,所以set可以保证元素key和value都唯一,sadd保证key下能有多个唯一的value
 *
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka数据
    val orderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)


    val orderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将数据转化为样例类
    val idToInfoDStream: DStream[(String, OrderInfo)] = orderDStream.mapPartitions(partition => {
      partition.map(record => {
        //将数据转化为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //create_time yyyy-MM-dd HH:mm:ss
        //补全create_date字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        //补全create_hour字段
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        //返回数据
        (orderInfo.id, orderInfo)
      })
    })

    //将order_id提出来放在元组的第一个位置
    val idToDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.mapPartitions(iter => {
      iter.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //5.使用fullouterjoin防止join不上的数据丢失
    val fullDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToInfoDStream.fullOuterJoin(idToDetailDStream)


    val noUserDStream: DStream[SaleDetail] = fullDStream.mapPartitions(iter => {
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      //创建集合用来存放关联上的数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建redis连接
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth(GmallConstants.PASSWORD)

      iter.foreach { case (orderId, (orderOpt, detailOpt)) =>
        //redisKey
        val orderInfoKey: String = "orderInfo" + orderId
        val orderDetailKey: String = "orderDetail" + orderId


        //a.判断是否有orderInfo数据
        if (orderOpt.isDefined) {
          //有orderInfo数据
          val orderInfo: OrderInfo = orderOpt.get
          //b.判断orderDetail是否为空
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(saleDetail)
          }
          //c.将orderInfo数据保存至Redis
          //将样例类转换为字符串
          val orderInfoJson: String = Serialization.write(orderInfo)
          //将orderInfo数据保存至Redis
          jedisClient.set(orderInfoKey, orderInfoJson)
          //设置过期时间
          jedisClient.expire(orderInfoKey, 100)

          //d.去orderDetailRedis缓存查数据
          if (jedisClient.exists(orderDetailKey)) {
            val detailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
            detailSet.asScala.foreach(detail => {
              //将redis查出来的数据转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
              details.add(new SaleDetail(orderInfo, orderDetail))
            })
          }
        } else {
          //没有orderInfo数据
          //a.拿到orderDetail数据
          val orderDetail: OrderDetail = detailOpt.get
          if (jedisClient.exists(orderInfoKey)) {
            //存在orderinfo数据
            //a.1获取orderinfo数据
            val infoJson: String = jedisClient.get(orderInfoKey)

            if (infoJson != null) {
              //a.2将orderInfoJson数据转换为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])

              //a.3将join上的数据添加到SaleDetail样例类里
              details.add(new SaleDetail(orderInfo, orderDetail))
            }
          } else {
            //不存在orderinfo数据
            //将自己（orderDetail）写入redis缓存
            //a.将样例类转换为Json
            val orderDetailJson: String = Serialization.write(orderDetail)
            //b.将数据写入redis
            jedisClient.sadd(orderDetailKey, orderDetailJson)
            //c.设置过期时间
            jedisClient.expire(orderDetailKey, 100)
          }
        }
      }
      //关闭连接
      jedisClient.close()
      details.asScala.toIterator
    }) //mapPartition结束


    //7.补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(partition => {
      //a.获取redis连接
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth(GmallConstants.PASSWORD)
      //b.查库
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //根据key获取数据
        val userInfoKey: String = "userInfo" + saleDetail.user_id
        val userInfoJson: String = jedisClient.get(userInfoKey)
        //将数据转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail //返回补充好的信息
      })
      //关闭连接
      jedisClient.close()
      details
    })

    //TODO 写入es
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        //索引名
        val indexName: String = GmallConstants.ES_DETAIL_INDEXNAME + "-" +
          sdf.format(new Date(System.currentTimeMillis())) //可以用part.next().dt代替,以防零点漂移
        val list: List[(String, SaleDetail)] = part.toList.map(x => (x.order_detail_id, x))
        MyEsUtil.insertBulk(indexName, list)
      })
    })

    saleDetailDStream.print()

    //TODO 开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}

object ss {
  def main(args: Array[String]): Unit = {
    val someInt: Option[Int] = None
    println(someInt.isDefined)
  }
}
