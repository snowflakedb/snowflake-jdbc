/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

abstract class ResultSetArrowForce0MultiTimeZone extends BaseJDBCTest
{
  static Object[][] testData()
  {
    // all tests in this class need to run for both query result formats json and arrow
    return new Object[][]{
        {"json", "UTC"},
        {"json", "America/New_York"},
        {"json", "MEZ"},
        {"arrow_force", "UTC"},
        {"arrow_force", "America/Los_Angeles"},
        {"arrow_force", "MEZ"},
        };
  }

  protected String queryResultFormat;
  protected String tz;

  ResultSetArrowForce0MultiTimeZone(String queryResultFormat, String timeZone)
  {
    this.queryResultFormat = queryResultFormat;
    System.setProperty("user.timezone", timeZone);
    this.tz = timeZone;
  }

  Connection init(String table, String column, String values) throws SQLException
  {
    Connection con = BaseJDBCTest.getConnection();

    try (Statement statement = con.createStatement())
    {
      statement.execute(
          "alter session set " +
          "TIMEZONE='America/Los_Angeles'," +
          "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ'," +
          "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
          "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
          "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
          "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    }

    con.createStatement().execute("alter session set jdbc_query_result_format" +
                                  " = '" + queryResultFormat + "'");
    con.createStatement().execute("create or replace table " + table + " " + column);
    con.createStatement().execute("insert into " + table + " values " + values);
    return con;
  }

  protected void finish(String table, Connection con) throws SQLException
  {
    con.createStatement().execute("drop table " + table);
    con.close();
    System.clearProperty("user.timezone");
  }
}
