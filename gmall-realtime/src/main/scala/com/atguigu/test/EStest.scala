package com.atguigu.test

import com.atguigu.utils.MyEsUtil
import io.searchbox.client.JestClient
import io.searchbox.core.Index
import java.util

/**
 * @ClassName gmall-parent-EStest 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月02日19:58 - 周四
 * @Describe
 */
object EStest {
  def main(args: Array[String]): Unit = {
    insertClass
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

  def Test() = {
    val client: JestClient = MyEsUtil.getClient


    client.close()
  }
}


//根据参数的样式定义样例类
case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]]) {

}


