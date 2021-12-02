package com.atguigu.bean

/**
 * @ClassName gmall-parent-EventLog 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日14:06 - 周四
 * @Describe 事件日志样例类
 */

/*
* 原始数据：
* {"area":"tianjin",
*  "uid":"139",
*  "itemid":29,
*  "npgid":17,
*  "evid":"coupon",
*  "os":"andriod",
*  "pgid":22,
*  "appid":"gmall2019",
*  "mid":"mid_177",
*  "type":"event",
*  "ts":1638258869786}
* */
case class EventLog(mid: String,
                    uid: String,
                    appid: String,
                    area: String,
                    os: String,
                    `type`: String,
                    evid: String,
                    pgid: String,
                    npgid: String,
                    itemid: String,
                    var logDate: String,
                    var logHour: String,
                    var ts: Long
                   )
