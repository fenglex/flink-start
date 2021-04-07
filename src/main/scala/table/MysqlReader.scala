package table

import java.sql.DriverManager
import java.util.Properties

import cn.hutool.db.DbUtil
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, WithOperations}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala.{createTypeInformation, _}


/**
 * @author haifeng
 * @version 1.0
 * @date 2021/4/6 13:50
 */


case class DataDict(id: Int, dataName: String, dataCode: String, freq: String, createTime: Int, updateTime: Int)

object MysqlReader {
  def main(args: Array[String]): Unit = {
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env, fsSettings)
    val properties = new Properties()
    properties.setProperty("username", "root")
    properties.setProperty("password", "lDwcgRITlBh71D1s")
    properties.setProperty("url", "jdbc:mysql://fenglex.com:3306/flink")
    properties.setProperty("sql",
      "select id,data_name,data_code,freq,create_time,update_time from tb_data_dict where id>'%s' order by id")
    properties.setProperty("limit", "10")
    val stream: DataStream[DataDict] = env.addSource(new MysqlSource(properties))
    //stream.print()
    val tab = tableEnv.fromDataStream[DataDict](stream)
    //tab.where($"id".eq("10"))
    //println(queryResult)
    env.execute()
    //tableEnv.execute("test")
  }

}


class MysqlSource(property: Properties) extends SourceFunction[DataDict] {
  var stop = false

  override def run(sourceContext: SourceFunction.SourceContext[DataDict]): Unit = {
    var empty = false
    while (!stop) {
      val connection = DriverManager.getConnection(property.getProperty("url"), property.getProperty("username"), property
        .getProperty("password"))
      var id = "0"
      while (!empty) {
        // select * from tb_table where id>%s order by id limit 10
        val statement = connection.createStatement()
        val sql = property.getProperty("sql") + " limit " + property.getProperty("limit");
        val querySql = String.format(sql, id);
        println(querySql)
        val rs = statement.executeQuery(querySql)
        empty = true
        while (rs.next()) {
          empty = false
          id = rs.getString("id")
          val dict = DataDict(rs.getInt("id"), rs.getString("data_name"), rs.getString("data_code"),
            rs.getString("freq"), rs.getInt("create_time"), rs.getInt("update_time"))
          sourceContext.collect(dict)
        }
        DbUtil.close(rs, statement)
        println("等待2秒钟")
        Thread.sleep(2000)
      }
      empty = false
      println("等待5秒钟,下次执行")
      Thread.sleep(5000)
      connection.close()
    }
  }

  override def cancel(): Unit = {
    stop = true
    println("close mysql source")
  }
}

