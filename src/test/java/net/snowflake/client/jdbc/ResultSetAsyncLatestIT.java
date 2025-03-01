package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Test AsyncResultSet */
@Tag(TestTags.RESULT_SET)
public class ResultSetAsyncLatestIT extends BaseJDBCTest {
  @Test
  public void testAsyncResultSet() throws SQLException {
    String queryID;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
        statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
        String createTableSql = "select * from test_rsmd";
        try (ResultSet rs =
            statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(createTableSql)) {
          queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        }
      } finally {
        statement.execute("drop table if exists test_rsmd");
      }
    }
    // Close and reopen connection

    try (Connection connection = getConnection();
        // open a new connection and create a result set
        ResultSet resultSet =
            connection.unwrap(SnowflakeConnection.class).createResultSet(queryID)) {
      // Process result set
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      SnowflakeResultSetMetaData secretMetaData =
          resultSetMetaData.unwrap(SnowflakeResultSetMetaData.class);
      assertEquals(
          secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
    }
  }
}
