package com.atguigu.test

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constans.GmallConstants
import com.atguigu.utils.MyEsUtil
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date
import java.{io, util}
import scala.collection.{breakOut, immutable}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * @ClassName gmall-parent-MapTest 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日15:53 - 周四
 * @Describe 同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。
 *           达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次预警。
 */
object MapTest {
  def main(args: Array[String]): Unit = {

    val list: List[(String, Iterable[EventLog])] = List(("mid_108", ArrayBuffer(
      EventLog("mid_108", "416", "gmall2019", "guangdong", "andriod", "event", "coupon", "15", "30", "9", "2021-12-02", "16", "1638432453698".toLong),
      EventLog("mid_108", "313", "gmall2019", "beijing", "andriod", "event", "coupon", "22", "10", "24", "2021-12-02", "16", "1638432453698".toLong),
      EventLog("mid_108", "222", "gmall2019", "hunan", "andriod", "event", "coupon", "6", "35", "38", "2021-12-02", "16", "1638432453698".toLong),
      EventLog("mid_313", "123", "gmall2019", "shandong", "andriod", "event", "coupon", "13", "40", "32", "2021-12-02", "16", "1638432453699".toLong))),
      ("mid_386", ArrayBuffer(EventLog("mid_386", "1111", "gmall2019", "guangdong", "andriod", "event", "coupon", "15", "30", "9", "2021-11-30", "16", "1638467970000".toLong),
        EventLog("mid_386", "2222", "gmall2019", "guangdong", "andriod", "event", "coupon", "15", "30", "9", "2021-11-30", "16", "1638467970000".toLong),
        EventLog("mid_386", "3333", "gmall2019", "guangdong", "andriod", "event", "coupon", "15", "30", "9", "2021-11-30", "16", "1638467970000".toLong))), (
        "mid_210", ArrayBuffer(EventLog("mid_110", "1111", "gmall2019", "guangdong", "andriod", "event", "clickItem", "15", "30", "9", "2021-12-01", "16", "1638432453698".toLong))
      ))


    val boolLogs = list.map {
      case (mid, logs) => {
        val uids: util.HashSet[String] = new util.HashSet[String]()
        val items: util.HashSet[String] = new util.HashSet[String]()
        val events: util.ArrayList[String] = new util.ArrayList[String]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

        var bool: Boolean = true
        breakable {
          //logs遍历必须要写在breakable,break()出去没有意义
          logs.map(log => {
            //发生过的行为,只要有就添加进去
            events.add(log.evid)

            //若用户有浏览商品的动作就退出循环,证明用户是活的,不需要预警
            if ("clickItem".equals(log.evid)) {
              bool = false
              break() //break是退出到breakable{...}外
            } else if ("coupon".equals(log.evid)) {
              //将领取了优惠券的商品加入到item列表中
              items.add(log.itemid)
              //同一个设备领取过优惠券的id加入到uids
              uids.add(log.uid)
            }
          })
        }
        /*
         * Q:这里为什么不能直接筛选出预警日志呢
         * A:因为若不满足条件的话就啥都没有返回，那DStream接收什么呢?
         * */
        if (uids.size() >= 3 && bool) {
          (CouponAlertInfo(mid, uids, items, events, logs.map(x => x.ts).head))
        } else {
          (CouponAlertInfo("-1", null, null, null, logs.map(x => x.ts).head))
        }
      }
    }
    val alterLogs: List[CouponAlertInfo] = boolLogs.filter(x => x.mid != "-1")
    //println(alterLogs)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val resList = alterLogs.map(x => {
      val indexName: String = GmallConstants.ES_ALERT_INDEX + "-" + sdf.format(new Date(x.ts)).split(" ")(0)
      val list: List[(String, CouponAlertInfo)] = List((x.mid + x.ts / 1000 / 60, x))
      MyEsUtil.insertBulk(indexName, list)
    })


    //println(boolLogs)

  }
}


/*
* breakable就相当于java中的break和continue
* */
object BreakableDemo {
  def main(args: Array[String]): Unit = {
    //此时相当于Java/C/C++中的break
    println("break")
    breakable {
      for (i <- 1 to 5) {
        if (i == 3) {
          break()
        }
        println(i)
      }
    }

    println("continue")
    //此时相当于Java/C/C++中的continue
    for (i <- 1 to 5) {
      breakable {
        if (i == 3) {
          break()
        }
        println(i)
      }
    }
  }
}

object NullDemo {
  def main(args: Array[String]): Unit = {

  }
}
