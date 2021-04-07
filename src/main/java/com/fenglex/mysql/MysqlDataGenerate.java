package com.fenglex.mysql;

import cn.hutool.core.date.DateUtil;
import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.log.level.Level;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/4/7 11:00
 */
public class MysqlDataGenerate {
    public static void main(String[] args) throws SQLException {
        DbUtil.setShowSqlGlobal(true, true, true, Level.DEBUG);
        HikariConfig config = new HikariConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setConnectionTestQuery("SELECT 1");
        config.setUsername("root");
        config.setPassword("lDwcgRITlBh71D1s");
        config.setJdbcUrl("jdbc:mysql://fenglex.com:3306/flink");
        //config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
        //config.addDataSourceProperty("url", "jdbc:mysql://nuc.fenglex.com:3306/flink");
// 创建数据源
        DataSource ds = new HikariDataSource(config);
        Db db = DbUtil.use(ds);
        for (int i = 100; i < 200; i++) {
            long timestamp = System.currentTimeMillis() / 1000;
            db.execute("insert into tb_data_dict (data_name,data_code,freq,create_time,update_time) values(?,?,?,?,?)"
                    , "数据名称" + i, "D" + i, "D", timestamp, timestamp);
        }
    }
}
