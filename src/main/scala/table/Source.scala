package table

import cn.hutool.db.DbUtil
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.sql.DriverManager
import java.util.Properties
import scala.collection.mutable.ListBuffer

class Source {

}

case class DataDict(id: Int, dataName: String, dataCode: String, freq: String, createTime: Int, updateTime: Int)

class MysqlSource(property: Properties) extends SourceFunction[DataDict] {
  var stop = false
  var lastUpdate = 0

  override def run(sourceContext: SourceFunction.SourceContext[DataDict]): Unit = {
    var empty = false
    val list = ListBuffer[DataDict]()
    while (!stop) {
      val connection = DriverManager.getConnection(property.getProperty("url"), property.getProperty("username"), property
        .getProperty("password"))
      var id = "0"
      while (!empty) {
        val statement = connection.createStatement()
        val sql = "select * from tb_data_dict where update_time>%s order by update_time asc"
        val querySql = String.format(sql, lastUpdate + "");
        println(querySql)
        val rs = statement.executeQuery(querySql)
        empty = true
        while (rs.next()) {
          empty = false
          lastUpdate = rs.getString("update_time").toInt
          val dict = DataDict(rs.getInt("id"), rs.getString("data_name"), rs.getString("data_code"),
            rs.getString("freq"), rs.getInt("create_time"), rs.getInt("update_time"))
          list.+=(dict)
        }
        DbUtil.close(rs, statement)
      }
      empty = false
      for (elem <- list) {
        sourceContext.collect(elem)
      }
      list.clear()
      println("等待5秒钟,下次执行")
      Thread.sleep(10000)
      connection.close()
    }
  }

  override def cancel(): Unit = {
    stop = true
    println("close mysql source")
  }
}


class ListSource() extends RichSourceFunction[(String, String)] {
  override def run(sourceContext: SourceFunction.SourceContext[(String, String)]): Unit = {
    while (true) {
      for (d <- 100 to 120) {
        sourceContext.collect(("D" + d, "这是数据" + d))
      }
    }

  }

  override def cancel(): Unit = {

  }
}
