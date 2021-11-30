package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.OrderInfo

import scala.util.parsing.json.JSONArray

/**
 * @ClassName gmall-parent-JsonTest 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日22:11 - 周二
 * @Describe
 */
object JsonTest {
  def main(args: Array[String]): Unit = {
    val kafkaJson:String ="{\"data\":[{\"id\":\"3\",\"consignee\":\"XH\",\"consignee_tel\":\"13506458453\",\"total_amount\":\"53087.0\",\"order_status\":\"1001\",\"user_id\":\"204\",\"payment_way\":null,\"delivery_address\":\"第5大街第6号楼4单元583门\",\"order_comment\":\"描述416156\",\"out_trade_no\":\"786771177358176\",\"trade_body\":\"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 6GB+128GB冰霜银全网通5G手机等10件商品\",\"create_time\":\"2020-06-20 16:12:39\",\"operate_time\":null,\"expire_time\":\"2020-06-20 16:27:39\",\"process_status\":null,\"tracking_no\":null,\"parent_order_id\":null,\"img_url\":\"http://img.gmall.com/748375.jpg\",\"province_id\":\"4\",\"activity_reduce_amount\":\"0.0\",\"coupon_reduce_amount\":\"0.0\",\"original_total_amount\":\"53079.0\",\"feight_fee\":\"8.0\",\"feight_fee_reduce\":null,\"refundable_time\":null}],\"database\":\"gmall\",\"es\":1638277770000,\"id\":1,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\",\"consignee\":\"varchar(100)\",\"consignee_tel\":\"varchar(20)\",\"total_amount\":\"decimal(10,2)\",\"order_status\":\"varchar(20)\",\"user_id\":\"bigint(20)\",\"payment_way\":\"varchar(20)\",\"delivery_address\":\"varchar(1000)\",\"order_comment\":\"varchar(200)\",\"out_trade_no\":\"varchar(50)\",\"trade_body\":\"varchar(200)\",\"create_time\":\"datetime\",\"operate_time\":\"datetime\",\"expire_time\":\"datetime\",\"process_status\":\"varchar(20)\",\"tracking_no\":\"varchar(100)\",\"parent_order_id\":\"bigint(20)\",\"img_url\":\"varchar(200)\",\"province_id\":\"int(20)\",\"activity_reduce_amount\":\"decimal(16,2)\",\"coupon_reduce_amount\":\"decimal(16,2)\",\"original_total_amount\":\"decimal(16,2)\",\"feight_fee\":\"decimal(16,2)\",\"feight_fee_reduce\":\"decimal(16,2)\",\"refundable_time\":\"datetime\"},\"old\":null,\"sql\":\"\",\"sqlType\":{\"id\":-5,\"consignee\":12,\"consignee_tel\":12,\"total_amount\":3,\"order_status\":12,\"user_id\":-5,\"payment_way\":12,\"delivery_address\":12,\"order_comment\":12,\"out_trade_no\":12,\"trade_body\":12,\"create_time\":93,\"operate_time\":93,\"expire_time\":93,\"process_status\":12,\"tracking_no\":12,\"parent_order_id\":-5,\"img_url\":12,\"province_id\":4,\"activity_reduce_amount\":3,\"coupon_reduce_amount\":3,\"original_total_amount\":3,\"feight_fee\":3,\"feight_fee_reduce\":3,\"refundable_time\":93},\"table\":\"order_info_test\",\"ts\":1638277851091,\"type\":\"INSERT\"}"

    val nObject: JSONObject = JSON.parseObject(kafkaJson)

    val info = nObject.getJSONArray("data")

    val orderInfo: OrderInfo = JSON.parseObject(info.get(0).toString, classOf[OrderInfo])

    println(orderInfo.id)

    
  }
}
