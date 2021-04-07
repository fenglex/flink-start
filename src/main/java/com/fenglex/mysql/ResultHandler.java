package com.fenglex.mysql;

import java.sql.ResultSet;

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/4/7 14:32
 */
public interface ResultHandler<T> {
    /**
     * 处理单行数据
     *
     * @param resultSet 。
     * @return 。
     */
    T handler(ResultSet resultSet);
}
