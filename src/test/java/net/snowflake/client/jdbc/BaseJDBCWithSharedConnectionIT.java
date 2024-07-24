package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseJDBCWithSharedConnectionIT extends BaseJDBCTest {

  protected static Connection connection;

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    connection = getConnection();
  }

  @AfterClass
  public static void closeConnection() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }
}
