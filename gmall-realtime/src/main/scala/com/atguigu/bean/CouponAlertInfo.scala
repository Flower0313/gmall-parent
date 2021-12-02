package com.atguigu.bean

/**
 * @ClassName gmall-parent-CouponAlertInfo 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日14:07 - 周四
 * @Describe 预警日志样例类
 */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long
                          )
