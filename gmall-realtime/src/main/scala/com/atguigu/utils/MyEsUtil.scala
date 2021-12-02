package com.atguigu.utils

import com.atguigu.bean.CouponAlertInfo
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

import java.util
import java.util.Objects
import scala.collection.mutable


/**
 * @ClassName gmall-parent-MyEsUtil 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日19:29 - 周四
 * @Describe
 */
object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
   * 获取客户端
   *
   * @return
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   *
   * @param client
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.close()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  private def build(): Unit = {
    factory = new JestClientFactory
    //编写配置
    val httpClientConfig: HttpClientConfig =
      new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
        .multiThreaded(true)
        .maxTotalConnection(20) //连接总数
        .connTimeout(10000) //连接等待时间
        .readTimeout(1000).build()
    factory.setHttpClientConfig(httpClientConfig)
  }


  /**
   * 尽管5分钟的窗口可能有多条重复的数据，因为采集周期为5s,而窗口为5min，所以肯定会有之前重复的数据进行计算，
   * 这就意味着写入到es中可能有重复数据，但为什么结果是没有呢?因为es的put的幂等性存在，同样的数据会覆盖，所以
   * 只会一直有一条相同的数据
   * @param indexName
   * @param docList
   */
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {
    //插入方式一
    /*if (docList.nonEmpty) {
      val client: JestClient = getClient
      for ((id, doc) <- docList) {
        val index: Index = new Index.Builder(doc) //底层会将样例对象转换成json
          .index(indexName)
          .`type`("_doc")
          .id(id).build()
        client.execute(index);
      }
      client.close()
    }*/

    //插入方式二
    if (docList.nonEmpty) {
      val client: JestClient = getClient
      //声明添加bulk
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType("_doc")
      //其实也就一条元素
      for ((id, doc) <- docList) {
        //直接扔doc是因为它已经是个样例类了，会自动转换成json
        val index: Index = new Index.Builder(doc).id(id).build()
        bulkBuilder.addAction(index)
      }

      //声明返回列表
      var items: util.List[BulkResult#BulkResultItem] = null
      //获取到插入的反馈
      try {
        items = client.execute(bulkBuilder.build()).getItems
      } finally {
        client.close() //最后一定要关闭
        println("更新了" + items.size() + "条数据!")
      }
    }
  }
}