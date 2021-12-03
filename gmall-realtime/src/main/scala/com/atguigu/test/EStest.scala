package com.atguigu.test

import com.atguigu.utils.MyEsUtil
import com.ibm.icu.text.SimpleDateFormat
import io.searchbox.client.JestClient
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import java.util.Date

/**
 * @ClassName gmall-parent-EStest 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日19:58 - 周四
 * @Describe
 */
object EStest {
  def main(args: Array[String]): Unit = {
    readData
  }

  def insertSimple() = {
    val client: JestClient = MyEsUtil.getClient

    var source =
      """
        |{
        | "id":"2016173805",
        | "name":"肖华",
        | "sex":"male"
        |}
        |""".stripMargin

    val index: Index = new Index.Builder(source)
      .index("student")
      .`type`("_doc")
      .id("1003").build()

    client.execute(index);
    client.close()
  }

  //添加一个样例类
  def insertClass(): Unit = {
    val client: JestClient = MyEsUtil.getClient

    var actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val actor1: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    actor1.put("id", "222")
    actor1.put("name", "杰克")

    actorList.add(actor1)


    val movie: Movie = new Movie(1010, "时空恋旅人", 10f, actorList)

    val index: Index = new Index.Builder(movie) //底层会将样例对象转换成json
      .index("movie_index")
      .`type`("movie")
      .id("315").build()

    client.execute(index);
    client.close()
  }

  def readData() = {
    println("进来了")
    val client: JestClient = MyEsUtil.getClient
    val mid: String = "mid_386"
    val qStr: String =
      s"""
         |{
         |    "query": {
         |      "bool": {
         |        "filter": {
         |          "term":{"mid":"${mid}"}
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

    val result: SearchResult = client.execute(search)
    var isIndexFirst: Boolean = false;


    if (result.getTotal == null) {
      isIndexFirst = true
    }
    if(!isIndexFirst){
      val hits: util.List[SearchResult#Hit[util.HashMap[String, Any], Void]] = result.getHits(classOf[util.HashMap[String, Any]])
      if (hits.iterator().hasNext && !isIndexFirst) {
        val decimal: BigDecimal = BigDecimal(hits.iterator().next().source.get("ts").toString)

        if ((1638466860000L / 1000) - decimal.toLong / 1000 >= 60) {
          println("超过了一分钟,可以执行写入...")
        }
      }
    }

    client.close()
  }
}


//根据参数的样式定义样例类
case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]]) {

}


