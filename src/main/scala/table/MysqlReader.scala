package table

import cn.hutool.db.DbUtil
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

import java.sql.DriverManager
import java.util.Properties


/**
 * @author haifeng
 * @version 1.0
 * @date 2021/4/6 13:50
 */
object MysqlReader {
  def main(args: Array[String]): Unit = {
    val fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
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
    tableEnv.createTemporaryView("tb_data_dict", stream)

    val sql = "select t.dataCode,t.dataName,t.num,t.batch from  (select *,row_number() over (partition by dataCode order by batch desc) num from tb_data_dict) t where t.num=1"
    val sqlResult = tableEnv.sqlQuery(sql)
    sqlResult.printSchema()
    tableEnv.createTemporaryView("dict",sqlResult)

    val dictSql="select * from dict"
    tableEnv.sqlQuery(dictSql).toRetractStream[(String,String,Long,Long)].filter(_._1).print()

    //sqlResult.toRetractStream[(String,String,Long,Long)].print()
    //tableEnv.createTemporaryView("tb_data_dict", stream)
    //    stream.print()
    //    val table = tableEnv.fromDataStream[DataDict](stream)
    //    val result = table.select($"id", $"dataName").filter($"id" > 100)
    //    result.toAppendStream[(Int, String)].print()
    //
    //    tableEnv.createTemporaryView("tb_data_dict", stream)
    //    val sqlResult = tableEnv.sqlQuery("select * from tb_data_dict where id>50 and id<60")
    //    sqlResult.toAppendStream[DataDict].print("sql result")
    //println(queryResult)
    env.execute("table test")
  }

}

case class DataDict(id: Int, dataName: String, dataCode: String, freq: String, createTime: Int, updateTime: Int,
                    batch: Long)


class MysqlSource(property: Properties) extends SourceFunction[DataDict] {
  var stop = false
  var batch = 1

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
            rs.getString("freq"), rs.getInt("create_time"), rs.getInt("update_time"), batch)
          sourceContext.collect(dict)
        }
        DbUtil.close(rs, statement)
        //println("等待2秒钟")
        Thread.sleep(2000)
      }
      empty = false
      println("等待5秒钟,下次执行")
      Thread.sleep(5000)
      connection.close()
      batch = batch + 1
    }
  }

  override def cancel(): Unit = {
    stop = true
    println("close mysql source")
  }
}

