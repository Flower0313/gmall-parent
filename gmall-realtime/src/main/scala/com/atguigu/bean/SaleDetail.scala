package com.atguigu.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @ClassName gmall-parent-SaleDetail 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日15:36 - 周五
 * @Describe 销售详情样例类，合并多个表,一共12个字段,和es上字段个数一样
 */

//可以看到这里有默认值,因为防止有些类没有join进来
case class SaleDetail(var order_detail_id: String = null,
                      var order_id: String = null,
                      var order_status: String = null,
                      var create_time: String = null,
                      var user_id: String = null,
                      var sku_id: String = null,
                      var user_gender: String = null,
                      var user_age: Int = 0,
                      var user_level: String = null,
                      var sku_price: Double = 0D,
                      var sku_name: String = null,
                      var dt: String = null
                     ) {
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
    /*
    * 若SaleDetail的主构造方法中的参数定义了默认值就可以直接调用this这个方法,若没有使用默认值,
    * 那么this中就需要填写参数,比如this(x1,x2,x3,x4....),
    * 这里的this方法就相当于初始化属性,赋的赋null值，赋的赋0
    * */
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)
  }

  //合并订单日志,其实就是将orderInfo表中的数据填入到SaleDetail对应的字段中
  def mergeOrderInfo(orderInfo: OrderInfo): Unit = {
    if (orderInfo != null) {
      this.order_id = orderInfo.id
      this.order_status = orderInfo.order_status
      this.create_time = orderInfo.create_time
      this.dt = orderInfo.create_date
      this.user_id = orderInfo.user_id
    }
  }

  //合并订单详情日志
  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
    if (orderDetail != null) {
      this.order_detail_id = orderDetail.id
      this.sku_id = orderDetail.sku_id
      this.sku_name = orderDetail.sku_name
      this.sku_price = orderDetail.order_price.toDouble
    }
  }

  //合并用户信息
  def mergeUserInfo(userInfo: UserInfo): Unit = {
    if (userInfo != null) {
      this.user_id = userInfo.id

      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date: Long = sdf.parse(userInfo.birthday).getTime //用户出生日期
      val curTs: Long = System.currentTimeMillis() //当前时间戳
      val betweenMs: Long = curTs - date
      //计算用户年龄
      val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L

      this.user_age = age.toInt
      this.user_gender = userInfo.gender
      this.user_level = userInfo.user_level
    }
  }
}































