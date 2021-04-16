package table

import cn.hutool.db.DbUtil
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

import java.sql.DriverManager
import java.util.Properties
import scala.collection.mutable.ListBuffer


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

    val sql = "select t.dataCode,t.dataName,t.createTime,t.updateTime from  (select *,row_number() over (partition by dataCode order by updateTime desc) num from tb_data_dict) t where t.num=1"
    val sqlResult = tableEnv.sqlQuery(sql)
    sqlResult.printSchema()
    tableEnv.createTemporaryView("dict", sqlResult)

    val dictSql = "select * from dict"
    tableEnv.sqlQuery(dictSql).toRetractStream[(String, String, Long, Long)].print()


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





