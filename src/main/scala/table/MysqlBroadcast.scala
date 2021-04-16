package table

import cn.hutool.core.util.RandomUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

import java.util.Properties

object MysqlBroadcast {

  def main(args: Array[String]): Unit = {
    val fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env, fsSettings)
    val properties = new Properties()
    properties.setProperty("password", "lDwcgRITlBh71D1s")
    properties.setProperty("username", "root")
    properties.setProperty("url", "jdbc:mysql://fenglex.com:3306/flink")
    val stream: DataStream[DataDict] = env.addSource(new MysqlSource(properties))
    tableEnv.createTemporaryView("tb_data_dict", stream)
    val sql = "select t.id,t.dataCode,t.dataName,t.createTime,t.updateTime from  (select *,row_number() over (partition by dataCode order by updateTime desc) num from tb_data_dict) t where t.num=1"

    tableEnv.sqlQuery(sql).toRetractStream[(Int, String, String, Int, Int)].print("data_dict")

    val data = env.socketTextStream("127.0.0.1", 8800).map((RandomUtil.randomInt(100), _))

    //val dataTable = tableEnv.fromDataStream(data, $"code")
    tableEnv.createTemporaryView("tb_data", data, $"id", $"data_code")
    tableEnv.sqlQuery("select * from tb_data").toAppendStream[(Int, String)].print("tb_data")

    val joinSql="select t1.data_code,t2.dataName,t2.updateTime from tb_data t1 left join tb_data_dict t2 on t1.data_code=t2.dataCode"
    tableEnv.sqlQuery(joinSql).toRetractStream[(String,String,Int)].print("join")
    env.execute("MysqlBroadcast")
  }
}

