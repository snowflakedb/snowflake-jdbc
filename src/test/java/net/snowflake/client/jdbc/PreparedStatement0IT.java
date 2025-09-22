package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Prepared statement integration tests */
abstract class PreparedStatement0IT extends BaseJDBCTest {
  // Unique table name per test class to prevent race conditions
  protected final String uniqueTableName = "test_prepst_" + SnowflakeUtil.randomAlphaNumeric(10);

  Connection init() throws SQLException {
    return BaseJDBCTest.getConnection();
  }

  protected Connection getConn(String queryResultFormat) throws SQLException {
    Connection conn = BaseJDBCTest.getConnection();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  final String insertSQL = "insert into " + uniqueTableName + " values(?, ?, ?, ?, ?, ?)";
  final String selectAllSQL = "select * from " + uniqueTableName;
  final String updateSQL = "update " + uniqueTableName + " set COLC = 'newString' where ID = ?";
  final String deleteSQL = "delete from " + uniqueTableName + " where ID = ?";
  final String selectSQL = "select * from " + uniqueTableName + " where ID = ?";
  final String createTableSQL =
      "create or replace table "
          + uniqueTableName
          + "(id INTEGER, "
          + "colA DOUBLE, colB FLOAT, colC String,  "
          + "colD NUMBER, col INTEGER)";
  final String deleteTableSQL = "drop table if exists " + uniqueTableName;
  final String enableCacheReuse = "alter session set USE_CACHED_RESULT=true";
  final String tableFuncSQL = "select 1 from table(generator(rowCount => ?))";

  @BeforeEach
  public void setUp() throws SQLException {
    try (Connection con = init()) {
      con.createStatement().execute(createTableSQL);
    }
  }

  @AfterEach
  public void tearDown() throws SQLException {
    try (Connection con = init()) {
      con.createStatement().execute(deleteTableSQL);
    }
  }
}
