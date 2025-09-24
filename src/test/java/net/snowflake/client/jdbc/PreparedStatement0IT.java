package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Prepared statement integration tests */
abstract class PreparedStatement0IT extends BaseJDBCTest {
  String insertSQLFormat = "insert into %s values(?, ?, ?, ?, ?, ?)";
  String selectAllSQLFormat = "select * from %s";
  String updateSQLFormat = "update %s set COLC = 'newString' where ID = ?";
  String deleteSQLFormat = "delete from %s where ID = ?";
  String selectSQLFormat = "select * from %s where ID = ?";
  String createTableSQLFormat =
      "create or replace table %s(id INTEGER, "
          + "colA DOUBLE, colB FLOAT, colC String,  "
          + "colD NUMBER, col INTEGER)";
  String deleteTableSQLFormat = "drop table if exists %s";
  String enableCacheReuse = "alter session set USE_CACHED_RESULT=true";
  String tableFuncSQL = "select 1 from table(generator(rowCount => ?))";

  protected String tableName = "";
  protected String insertSQL = "";
  protected String deleteSQL = "";
  protected String selectAllSQL = "";
  protected String updateSQL = "";
  protected String deleteTableSQL = "";
  protected String selectSQL = "";
  protected String createTableSQL = "";

  PreparedStatement0IT(String tablePostfix) {
    // prefixes distinguish tables from subordinate classes allowing them to run in parallel to prevent race conditions
    initQueries(tablePostfix);
  }

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

  private String initQuery(String format) {
    return String.format(format, tableName);
  }

  private void initQueries(String postfix) {
    tableName = !postfix.isEmpty() ? String.format("TEST_PREPST_%s", postfix) : "TEST_PREPST";
    insertSQL = initQuery(insertSQLFormat);
    selectAllSQL = initQuery(selectAllSQLFormat);
    updateSQL = initQuery(updateSQLFormat);
    deleteSQL = initQuery(deleteSQLFormat);
    selectSQL = initQuery(selectSQLFormat);
    createTableSQL = initQuery(createTableSQLFormat);
    deleteTableSQL = initQuery(deleteTableSQLFormat);
  }
}
