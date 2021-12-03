package com.atguigu.test

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter}
import scala.collection.{immutable, mutable}
import scala.math.Ordered.orderingToOrdered

/**
 * @ClassName gmall-parent-SaleDetailTest 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日16:39 - 周五
 * @Describe 需求四测试案例
 */
object SaleDetailTest {
  def main(args: Array[String]): Unit = {
    val orderInfoStr1: String =
      """
        |{"payment_way":"","delivery_address":"第5大街第6号楼4单元583门","refundable_time":"","order_comment":"描述416156","feight_fee_reduce":"","original_total_amount":"53079.0","order_status":"1001","consignee_tel":"13506458453","trade_body":"华为商品","id":"1","parent_order_id":"","operate_time":"","consignee":"flower","create_time":"2020-06-20 16:12:39","expire_time":"2020-06-20 16:27:39","coupon_reduce_amount":"0.0","out_trade_no":"786771177358176","tracking_no":"","total_amount":"53087.0","user_id":"111","img_url":"google.com","province_id":"4","process_status":"","feight_fee":"8.0","activity_reduce_amount":"0.0"}
        |""".stripMargin

    val orderInfoStr2: String =
      """
        |{"payment_way":"","delivery_address":"沪松路7弄","refundable_time":"","order_comment":"rnm退钱","feight_fee_reduce":"","original_total_amount":"5199.0","order_status":"1002","consignee_tel":"13506458453","trade_body":"iphone13 mini","id":"2","parent_order_id":"","operate_time":"","consignee":"holden","create_time":"2020-06-20 16:12:39","expire_time":"2020-06-20 16:27:39","coupon_reduce_amount":"0.0","out_trade_no":"786771177358176","tracking_no":"","total_amount":"53087.0","user_id":"222","img_url":"baidu.com","province_id":"5","process_status":"","feight_fee":"8.0","activity_reduce_amount":"0.0"}
        |""".stripMargin

    val orderDetailStr1: String =
      """
        |{"sku_num":"2","img_url":"bilibili.com","sku_id":"5","sku_name":"肖华牌手机","order_price":"244.0","id":"1002","order_id":"1"}
        |""".stripMargin

    val orderDetailStr2: String =
      """
        |{"sku_num":"2","img_url":"bilibili.com","sku_id":"5","sku_name":"肖华牌手机","order_price":"244.0","id":"1002","order_id":"3"}
        |""".stripMargin


    val orderDetail1: OrderDetail = JSON.parseObject(orderDetailStr1, classOf[OrderDetail])
    val orderDetail2: OrderDetail = JSON.parseObject(orderDetailStr2, classOf[OrderDetail])
    val orderInfo1: OrderInfo = JSON.parseObject(orderInfoStr1, classOf[OrderInfo])
    orderInfo1.create_date = orderInfo1.create_time.split(" ")(0)
    orderInfo1.create_hour = orderInfo1.create_time.split(" ")(1).split(":")(0)

    val orderInfo2: OrderInfo = JSON.parseObject(orderInfoStr2, classOf[OrderInfo])
    orderInfo2.create_date = orderInfo2.create_time.split(" ")(0)
    orderInfo2.create_hour = orderInfo2.create_time.split(" ")(1).split(":")(0)

    //模拟fullOutJoin
    val list: immutable.Seq[(Int, (Option[OrderDetail], Option[OrderInfo]))] =
      List((1, (Some(orderDetail1), Some(orderInfo1))),
        (2, (None, Some(orderInfo2))), (3, (Some(orderDetail2), None)))


    //TODO 开始转换

    //隐式参数,需要给Serialization.write供用,作用是辅助将样例类转换为json字符串
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

    //定义销售类的集合,已经关联好的orderInfo和orderDetail可以写入其中
    val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]

    val jedisClient: Jedis = new Jedis("hadoop102", 6379)
    jedisClient.auth("w654646")
    //与map的区别是,map需要返回一个元素,foreach可以直接输出
    val joinUserDStream: immutable.Seq[Iterator[SaleDetail]] = list.map {
      case (orderId, (detailOpt, orderOpt)) => {
        val orderInfoKey: String = "orderInfo" + orderId
        val orderDetailKey: String = "orderDetail" + orderId

        //TODO A.a.若订单不为空,若是None则为false,这就是Option的方法
        if (orderOpt.isDefined) {
          //从Some中get出里面的元素,Some(5)=>5
          val orderInfo: OrderInfo = orderOpt.get
          //TODO A.b.若订单详细也不为空
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            //将orderInfo和orderDetail拼接成SaleDetail
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            //将关联好的saleDetail类直接写入
            details.add(saleDetail)
          }
          //TODO A.c.将orderInfo保存至Redis,为什么不只保存没有关联的orderInfo呢?因为orderInfo可以对应多个orderDetail,所以都要存
          //将样例类转换为json字符串
          val orderInfoJson: String = Serialization.write(orderInfo)
          //使用set的话相同的key会进行覆盖,因为订单id肯定是唯一的
          jedisClient.set(orderInfoKey, orderInfoJson)
          //设置过期时间,100秒后过期,若想更保证点就可以拉长时间
          jedisClient.expire(orderInfoKey, 100)

          //TODO A.d.去orderDetailRedis缓存查有没有能关联上的orderDetail
          if (jedisClient.exists(orderDetailKey)) { //exists可以判断redis是否有这个key
            //这里调用的smembers所以一个key里面肯定只存了唯一的value,因为一个订单可以有多个订单详情
            val detailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
            //asScala可以将util.Set转换为mutable.Set,将满足条件的订单详情全部关联
            detailSet.asScala.foreach(detail => {
              val orderDetail: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
              //若缓存中还有能关联的订单详细就继续连接
              details.add(new SaleDetail(orderInfo, orderDetail))
            })
          }
        } else { //TODO B.a 若传进来的本条数据中没有orderInfo,就拿orderDetail的数据,而且是一定会有,因为我们这里是fullOuterJoin,不可能都会为None
          val orderDetail: OrderDetail = detailOpt.get
          if (jedisClient.exists(orderInfoKey)) {
            //TODO B.b redis中存在对应的orderInfo数据,这里取出的就是唯一的orderInfoKey
            //按正常逻辑是能取到值的,因为每个orderInfo都存了进去,但是若是这条数据延迟>100,那就取不到了
            val infoJson: String = jedisClient.get(orderInfoKey)

            if (infoJson != null) {
              //TODO B.c 将orderInfoJson数据转换为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])

              //TODO B.d 将取出的orderInfo与详细表拼接
              details.add(new SaleDetail(orderInfo, orderDetail))
            }
          } else {
            //TODO B.e redis中不存在orderInfo数据,那就将没关联上的orderDetail转为json存入redis,因为可能传输有延迟
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(orderDetailKey, orderDetailJson) //写入redis
            jedisClient.expire(orderDetailKey, 100) //一般超过了这个时间就是真的没关联对象了
          }
        }
      }
        //关闭连接
        jedisClient.close()
        //将util.Array转换为mutable.Array然后转为Iterator
        details.asScala.toIterator //返回的是一堆这个,这里全是关联上的元素
    }

    //补全用户信息
    val saleDetailDStream: immutable.Seq[SaleDetail] = joinUserDStream.map(part => {
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      jedisClient.auth("w654646")
      val saleDetail: SaleDetail = part.next()
      val userInfoKey: String = "userInfo" + saleDetail.user_id
      val userInfoJson: String = jedisClient.get(userInfoKey)
      val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
      saleDetail.mergeUserInfo(userInfo)
      jedisClient.close()
      saleDetail//返回补充好的信息
    })

    //TODO C.写入es


  }
}

object OrderDetailTest {
  def main(args: Array[String]): Unit = {
    val jedisClient: Jedis = new Jedis("hadoop102", 6379)
    jedisClient.auth("w654646")

    val infoJson: String = jedisClient.get("k2")
    if (infoJson == null) {
      println("空")
    }


    jedisClient.close()
  }
}

/*case class Student(var name: String, var age: Int) {

}*/
