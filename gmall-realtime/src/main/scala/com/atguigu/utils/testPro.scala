package com.atguigu.utils

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

/**
 * @ClassName gmall-parent-testPro 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日18:14 - 周一
 * @Describe
 */
object testPro {
  def main(args: Array[String]): Unit = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm")

    val ts: Long = 1638432453698L / 1000 / 60
    val str: String = sdf.format(new Date(ts))
    println(str)
  }
}
