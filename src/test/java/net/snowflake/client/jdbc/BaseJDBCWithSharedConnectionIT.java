package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseJDBCWithSharedConnectionIT extends BaseJDBCTest {

  protected static Connection connection;

  @BeforeAll
  public static void setUpConnection() throws SQLException {
    connection = getConnection();
  }

  @AfterAll
  public static void closeConnection() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  public Statement createStatement(String queryResultFormat) throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    return stmt;
  }
}
