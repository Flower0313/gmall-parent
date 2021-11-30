package com.atguigu.bean

/**
 * @ClassName gmall-parent-OrderInfo 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日20:04 - 周二
 * @Describe
 */
case class OrderInfo(
                      id: String,
                      province_id: String,
                      consignee: String,
                      order_comment: String,
                      var consignee_tel: String,
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String,
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                      var create_date: String,
                      var create_hour: String)
/*
* 以下字段没有:
* process_status | activity_reduce_amount | coupon_reduce_amount | original_total_amount
* feight_fee | feight_fee_reduce | refundable_time
* */
