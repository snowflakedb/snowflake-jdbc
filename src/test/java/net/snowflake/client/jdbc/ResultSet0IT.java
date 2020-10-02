/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;

/** Result set test base class. */
public class ResultSet0IT extends BaseJDBCTest {
  private final String queryResultFormat;

  public Connection init(int injectSocketTimeout) throws SQLException {
    Connection connection = BaseJDBCTest.getConnection(injectSocketTimeout);

    Statement statement = connection.createStatement();
    statement.execute(
        "alter session set "
            + "TIMEZONE='America/Los_Angeles',"
            + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
            + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    statement.close();
    return connection;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    Statement stmt = conn.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    stmt.close();
    return conn;
  }

  public Connection init(Properties paramProperties) throws SQLException {
    Connection conn =
        BaseJDBCTest.getConnection(DONT_INJECT_SOCKET_TIMEOUT, paramProperties, false, false);
    Statement stmt = conn.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    stmt.close();
    return conn;
  }

  @Before
  public void setUp() throws SQLException {
    Connection con = init();

    // TEST_RS
    con.createStatement().execute("create or replace table test_rs (colA string)");
    con.createStatement().execute("insert into test_rs values('rowOne')");
    con.createStatement().execute("insert into test_rs values('rowTwo')");
    con.createStatement().execute("insert into test_rs values('rowThree')");

    // ORDERS_JDBC
    Statement statement = con.createStatement();
    statement.execute(
        "create or replace table orders_jdbc"
            + "(C1 STRING NOT NULL COMMENT 'JDBC', "
            + "C2 STRING, C3 STRING, C4 STRING, C5 STRING, C6 STRING, "
            + "C7 STRING, C8 STRING, C9 STRING) "
            + "stage_file_format = (field_delimiter='|' "
            + "error_on_column_count_mismatch=false)");
    // put files
    assertTrue(
        "Failed to put a file",
        statement.execute(
            "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @%orders_jdbc"));
    assertTrue(
        "Failed to put a file",
        statement.execute(
            "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE_2) + " @%orders_jdbc"));

    int numRows = statement.executeUpdate("copy into orders_jdbc");

    assertEquals("Unexpected number of rows copied: " + numRows, 73, numRows);

    con.close();
  }

  @After
  public void tearDown() throws SQLException {
    Connection con = init();
    con.createStatement().execute("drop table if exists orders_jdbc");
    con.createStatement().execute("drop table if exists test_rs");
    con.close();
  }

  ResultSet numberCrossTesting() throws SQLException {
    Connection con = init();
    Statement statement = con.createStatement();

    statement.execute(
        "create or replace table test_types(c1 number, c2 integer, c3 float, c4 boolean,"
            + "c5 char, c6 varchar, c7 date, c8 datetime, c9 time, c10 timestamp_ltz, "
            + "c11 timestamp_tz, c12 binary)");
    statement.execute(
        "insert into test_types values (null, null, null, null, null, null, null, null, null, null, "
            + "null, null)");
    statement.execute(
        "insert into test_types values(2, 5, 3.5, true,"
            + "'1','1', '1994-12-27', "
            + "'1994-12-27 05:05:05', '05:05:05', '1994-12-27 05:05:05', '1994-12-27 05:05:05', '48454C4C4F')");
    statement.execute("insert into test_types (c5, c6) values('h', 'hello')");
    return statement.executeQuery("select * from test_types");
  }

  ResultSet0IT(String queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }
}
