package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @ClassName gmall-parent-PropertiesUtil 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日15:24 - 周一
 * @Describe 读取配置文件
 */
object PropertiesUtil {
  def load(propertieName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}
