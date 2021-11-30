package com.atguigu.bean

/**
 * @ClassName gmall-parent-StartUpLog 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日15:43 - 周一
 * @Describe 样例类
 */
case class StartUpLog(
                       var mid:String, //设备id
                       var uid:String, //用户id
                       var appid:String, //应用程序id
                       var area:String, //地区
                       var os:String,  //操作系统
                       var ch:String,  //
                       var `type`:String, //类型
                       var vs:String,  //访问时间
                       var logDate:String, //登陆日期
                       var logHour:String, //登陆时间的时
                       var ts:Long //时间戳
                     )
