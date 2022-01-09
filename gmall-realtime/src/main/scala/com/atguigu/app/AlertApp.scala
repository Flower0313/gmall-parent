package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}

/**
 * @ClassName gmall-parent-AlertApp 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日14:08 - 周四
 * @Describe 需求三：预警业务类，数据来源于脚本生成,并手动维护kafka偏移量
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    //读取偏移量,只会执行一次,因为是在Driver端
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(GmallConstants.KAFKA_TOPIC_EVENT, "bigdata2021")
    //Attention 若redis有偏移量就从偏移量的位置开始读,若没有就从最新的开始读取
    if (offsetMap != null && offsetMap.nonEmpty) {
      println("offsetMap" + offsetMap)
      kafkaDStream = MyKafkaUtil.getKafkaOffsetStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc, offsetMap)
    } else {
      println("offsetMap" + offsetMap)
      kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    }

    //Step 2.2获取当前采集周期从Kafka中消费数据的起始偏移量以及结束偏移量
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform {
      rdd => {
        //因为kafkaDStream底层封装的是KafkaRDD,混入了这个HasOffsetRanges特质,这个特质中提供了可以获取偏移量的方法
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //读什么返回什么,这个只是获取点偏移量内容而已
        rdd
      }
    }


    //4.将数据转化成样例类(EventLog文档中有)，补充时间字段，将数据转换为（k，v） k->mid  v->log
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = offsetDStream.map(record => {
      //将数据转化为样例类
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      //补充日期，小时字段
      eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
      eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)

      (eventLog.mid, eventLog)
    })

    //5.开窗5min,累积5分钟的数据再聚合
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.分组聚合按照mid
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    //midToLogIterDStream.print() //这里可以输出
    //7.筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）
    val boolDStream: DStream[CouponAlertInfo] = midToLogIterDStream.mapPartitions(iter => {
      iter.map { case (mid, iter) =>
        //创建set集合用来保存uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建set集合用来保存优惠券所涉及商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建List集合用来保存用户行为事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //标志位
        var bool: Boolean = true
        //判断有没有浏览商品行为
        breakable {
          iter.foreach(log => {
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) { //判断用户是否有浏览商品行为
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) { //判断用户是否有领取购物券行为
              itemIds.add(log.itemid)
              uids.add(log.uid)
            }
          })
        }
        /*
        * Q1:这里为什么不能直接筛选出预警日志呢
        * A2:因为若不满足条件的话就啥都没有返回，那DStream接收什么呢?
        *
        * Q2:为什么一直不出结果呢?
        * A2:因为数据都没有满足这个条件,所以我们可以先将条件调小一点
        * */
        println("uids:" + uids.size() + "||bool:" + bool)
        var nowTime: Long = iter.map(x => x.ts).head
        if (uids.size() >= 3 && bool) { //目前调成了1
          (CouponAlertInfo(mid, uids, itemIds, events, nowTime))
        } else {
          (CouponAlertInfo("-1", null, null, null, nowTime))
        }
      }
    })

    //过滤掉不需要预警的数据
    val alterDStream: DStream[CouponAlertInfo] = boolDStream.filter(x => x.mid != "-1")

    alterDStream.print()

    alterDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        /*//val longs: Iterator[Long] = iter.map(x => x.ts)
        //拼接index,这里的时间可能存在零点漂移的问题,"gmall_coupon_alert-2021-12-02",可以试着改成iter.map(x => x.ts).toList.head
        val indexName: String = GmallConstants.ES_ALERT_INDEX + "-" + sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        //若不toList,返回的类型就是Iterator[(String,CouponAlertInfo)]
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(alert => {
          /*
          * Q:为什么需要这么写呢?
          * A:因为es上的数据是幂等性的，这样拼接id的意思就是每分钟的id名称都不一样，在一分钟内id就是一样的，
          *   保证了每分钟一份,所以就实现了每个设备每分钟预警一次
          * */
          (alert.mid + alert.ts / 1000 / 60, alert)
        })
        MyEsUtil.insertBulk(indexName, list)*/
      })

      OffsetManagerUtil.saveOffset(GmallConstants.KAFKA_TOPIC_EVENT, "bigdata2021", ranges)

    })


    //10.开启
    ssc.start()
    ssc.awaitTermination()


  }
}
