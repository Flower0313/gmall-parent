package com.atguigu.bean

/**
 * @ClassName gmall-parent-OrderDetail 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日15:29 - 周五
 * @Describe 订单详细
 */
case class OrderDetail(id: String,
                       order_id: String,
                       sku_name: String,
                       sku_id: String,
                       order_price: String,
                       img_url: String,
                       sku_num: String
                      )
