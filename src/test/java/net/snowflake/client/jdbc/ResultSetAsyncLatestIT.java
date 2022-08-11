/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.*;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Test AsyncResultSet */
@Category(TestCategoryResultSet.class)
public class ResultSetAsyncLatestIT extends BaseJDBCTest {
  @Test
  public void testAsyncResultSet() throws SQLException {
    String queryID;
    Connection connection = getConnection();
    try (Statement statement = connection.createStatement()) {
      statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
      statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
      String createTableSql = "select * from test_rsmd";
      ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(createTableSql);
      queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
      statement.execute("drop table if exists test_rsmd");
      rs.close();
    }
    // Close and reopen connection
    connection.close();
    connection = getConnection();
    // open a new connection and create a result set
    ResultSet resultSet = connection.unwrap(SnowflakeConnection.class).createResultSet(queryID);
    // Process result set
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    SnowflakeResultSetMetaData secretMetaData =
        resultSetMetaData.unwrap(SnowflakeResultSetMetaData.class);
    assertEquals(
        secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
    // Close statement and resultset
    resultSet.getStatement().close();
    resultSet.close();
    connection.close();
  }
}
