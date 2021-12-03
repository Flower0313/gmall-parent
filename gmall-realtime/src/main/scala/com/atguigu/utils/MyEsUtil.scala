package com.atguigu.utils

import com.atguigu.bean.CouponAlertInfo
import com.ibm.icu.text.SimpleDateFormat
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}

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
   *
   * @param indexName
   * @param docList
   */
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {
    if (docList.nonEmpty) {
      val client: JestClient = getClient
      //声明添加bulk
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType("_doc")
      //其实也就一条元素
      for ((id, doc) <- docList) {
        //id相同的话就保证了幂等性
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

  //额外的写法，每分钟预警一次
  def insertBulk2(indexName: String, docList: List[(String, Any)]): Unit = {
    if (docList.nonEmpty) {
      val client: JestClient = getClient
      val alertInfo: CouponAlertInfo = docList.map(x => x._2).head.asInstanceOf[CouponAlertInfo]
      var items: util.List[BulkResult#BulkResultItem] = null //声明返回列表,内部类
      var remoteTs: BigDecimal = null //远程时间
      var isIndexFirst: Boolean = false //是否有索引存在
      var isRemoteHave: Boolean = true //是否有同设备id
      var isOver60: Boolean = false

      //获取同设备的远端时间戳，比较间隔是否超过了1分钟
      val qStr: String =
        s"""
           |{
           |    "query": {
           |      "bool": {
           |        "filter": {
           |          "term":{"mid":"${alertInfo.mid}"}
           |        }
           |      }
           |    },
           |    "_source": ["mid","ts"]
           |}
           |""".stripMargin

      val search: Search = new Search.Builder(qStr)
        .addIndex("gmall_coupon_alert-query")
        .addType("_doc")
        .build()

      //TODO 获取查询结果
      val result: SearchResult = client.execute(search)
      //标志es是否含有这个索引,没有的话不能获取远程时间,应直接插入
      isIndexFirst = if (result.getTotal == null) true else false


      //若远程有这个index主题并且主题中有重复元素,就需要获取下来判断时间
      if (!isIndexFirst) {
        //TODO 解析查询结果
        val hits: util.List[SearchResult#Hit[util.HashMap[String, Any], Void]] = result
          .getHits(classOf[util.HashMap[String, Any]])
        //远程是否有同设备id,有的话需要比较时间再插入,没有的话直接插入
        isRemoteHave = hits.iterator().hasNext
        if (isRemoteHave) {
          remoteTs = BigDecimal(hits.iterator().next().source.get("ts").toString)
          isOver60 = ((alertInfo.ts.toLong / 1000) - remoteTs.toLong / 1000 >= 60)
        }
      }

      //TODO 插入条件1:当远程没有Index索引库时
      //TODO 插入条件2:当远程有Index库，但没有相同的设备ID时
      //TODO 插入条件3:当远程有Index库，且有相同设备ID并且时间超过了1分钟
      if (isIndexFirst || !isRemoteHave || (isRemoteHave && isOver60)) {
        //声明添加bulk
        val bulkBuilder: Bulk.Builder = new Bulk.Builder()
          .defaultIndex(indexName)
          .defaultType("_doc")
        for ((id, doc) <- docList) { //其实就是一条元素
          //直接扔doc是因为它已经是个样例类了，会自动转换成json
          val index: Index = new Index.Builder(doc).id(id).build()
          bulkBuilder.addAction(index)
        }
        //获取到插入的反馈
        try {
          items = client.execute(bulkBuilder.build()).getItems
        } finally {
          client.close() //最后一定要关闭
          println("更新了" + items.size() + "条数据!")
        }
      }
      client.close()
    }
  }
}