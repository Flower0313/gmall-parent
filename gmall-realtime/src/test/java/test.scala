
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisShardInfo}


/**
 * @ClassName gmall-parent-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日18:40 - 周一
 * @Describe
 */
object test {
  def main(args: Array[String]): Unit = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm")
    val str: String = sdf.format(new Date(1638256320970L))
    println(str)

  }
}
