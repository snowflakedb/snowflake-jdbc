/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.junit.After;
import org.junit.Before;

abstract class ResultSetArrowForce0MultiTimeZone extends BaseJDBCTest {
  static List<Object[]> testData() {
    String[] timeZones = new String[] {"UTC", "America/New_York", "MEZ"};
    String[] queryFormats = new String[] {"json", "arrow"};
    List<Object[]> ret = new ArrayList<>();
    for (String queryFormat : queryFormats) {
      for (String timeZone : timeZones) {
        ret.add(new Object[] {queryFormat, timeZone});
      }
    }
    return ret;
  }

  protected final String queryResultFormat;
  protected final String tz;
  private TimeZone origTz;

  ResultSetArrowForce0MultiTimeZone(String queryResultFormat, String timeZone) {
    this.queryResultFormat = queryResultFormat;
    this.tz = timeZone;
  }

  @Before
  public void setUp() {
    origTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(this.tz));
  }

  @After
  public void tearDown() {
    TimeZone.setDefault(origTz);
  }

  Connection init(String table, String column, String values) throws SQLException {
    Connection con = BaseJDBCTest.getConnection();

    try (Statement statement = con.createStatement()) {
      statement.execute(
          "alter session set "
              + "TIMEZONE='America/Los_Angeles',"
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    }

    con.createStatement()
        .execute("alter session set jdbc_query_result_format" + " = '" + queryResultFormat + "'");
    con.createStatement().execute("create or replace table " + table + " " + column);
    con.createStatement().execute("insert into " + table + " values " + values);
    return con;
  }

  protected void finish(String table, Connection con) throws SQLException {
    con.createStatement().execute("drop table " + table);
    con.close();
    System.clearProperty("user.timezone");
  }
}
