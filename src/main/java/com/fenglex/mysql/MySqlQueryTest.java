package com.fenglex.mysql;

import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.handler.RsHandler;
import cn.hutool.log.level.Level;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/4/7 13:48
 */
public class MySqlQueryTest {
    public static void main(String[] args) throws SQLException {
        DbUtil.setShowSqlGlobal(true, true, true, Level.DEBUG);
        HikariConfig config = new HikariConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setConnectionTestQuery("SELECT 1");
        config.setUsername("root");
        config.setPassword("lDwcgRITlBh71D1s");
        config.setJdbcUrl("jdbc:mysql://fenglex.com:3306/flink");
        DataSource ds = new HikariDataSource(config);
        Db db = DbUtil.use(ds);

        //System.out.println(string);
    }
}
