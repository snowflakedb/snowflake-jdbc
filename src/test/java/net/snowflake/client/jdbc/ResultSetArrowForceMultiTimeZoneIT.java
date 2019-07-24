package net.snowflake.client.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Completely compare json and arrow resultSet behaviors
 */
@RunWith(Parameterized.class)
public class ResultSetArrowForceMultiTimeZoneIT extends BaseJDBCTest
{
  @Parameterized.Parameters
  public static Object[][] data()
  {
    // all tests in this class need to run for both query result formats json and arrow
    if (BaseJDBCTest.isArrowTestsEnabled())
    {
      return new Object[][]{
          {"json", "UTC"},
          {"json", "America/Los_Angeles"},
          {"json", "America/New_York"},
          {"json", "Pacific/Honolulu"},
          {"json", "Asia/Singapore"},
          {"json", "MEZ"},
          {"json", "MESZ"},
          {"arrow", "UTC"},
          {"arrow", "America/Los_Angeles"},
          {"arrow", "America/New_York"},
          {"arrow", "Pacific/Honolulu"},
          {"arrow", "Asia/Singapore"},
          {"arrow", "MEZ"},
          {"arrow", "MESZ"}
      };
    }
    else
    {
      return new Object[][]{
          {"json", "UTC"},
          {"json", "America/Los_Angeles"},
          {"json", "America/New_York"},
          {"json", "Pacific/Honolulu"},
          {"json", "Asia/Singapore"},
          {"json", "MEZ"},
          {"json", "MESZ"},
          {"arrow", "UTC"},
          {"arrow", "America/Los_Angeles"},
          {"arrow", "America/New_York"},
          {"arrow", "Pacific/Honolulu"},
          {"arrow", "Asia/Singapore"},
          {"arrow", "MEZ"},
          {"arrow", "MESZ"}
      };
    }
  }

  private static String queryResultFormat;

  public ResultSetArrowForceMultiTimeZoneIT(String queryResultFormat, String timeZone)
  {
    this.queryResultFormat = queryResultFormat;
    System.setProperty("user.timezone", timeZone);
  }

  private Connection init(String table, String column, String values) throws SQLException
  {
    Connection con = getConnection();
    con.createStatement().execute("create or replace table " + table + " " + column);
    con.createStatement().execute("insert into " + table + " values " + values);
    return con;
  }

  private void finish(String table, Connection con) throws SQLException
  {
    con.createStatement().execute("drop table " + table);
    con.close();
    System.clearProperty("user.timezone");
  }

  public static Connection getConnection()
  throws SQLException
  {
    Connection conn = getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    if (isArrowTestsEnabled())
    {
      conn.createStatement().execute("alter session set query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @Test
  public void testTimestampNTZ() throws SQLException
  {
    for(int scale = 0; scale <= 9; scale++)
    {
      testTimestampNTZWithScale(scale);
    }
  }

  public void testTimestampNTZWithScale(int scale) throws SQLException
  {
    String[] cases = {
        "2017-01-01 12:00:00",
        "2014-01-02 16:00:00",
        "2014-01-02 12:34:56",
        "0001-01-02 16:00:00",
        "1969-12-31 23:59:59",
        "1970-01-01 00:00:00",
        "1970-01-01 00:00:01",
        "9999-01-01 00:00:00"
    };

    String[] results = {
        "Sun, 01 Jan 2017 12:00:00 Z",
        "Thu, 02 Jan 2014 16:00:00 Z",
        "Thu, 02 Jan 2014 12:34:56 Z",
        "Sun, 02 Jan 0001 16:00:00 Z",
        "Wed, 31 Dec 1969 23:59:59 Z",
        "Thu, 01 Jan 1970 00:00:00 Z",
        "Thu, 01 Jan 1970 00:00:01 Z",
        "Fri, 01 Jan 9999 00:00:00 Z"
    };

    String table = "test_arrow_ts_ntz";

    String column = "(a timestamp_ntz("+scale+"))";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while(i < cases.length)
    {
      rs.next();
      assertEquals(results[i++], rs.getString(1));
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampNTZWithNanos() throws SQLException
  {
    String[] cases = {
        "2017-01-01 12:00:00.123456789",
        "2014-01-02 16:00:00.0123",
        "2014-01-02 12:34:56.234241234",
        "0001-01-02 16:00:00.999999999",
        "1969-12-31 23:59:59.000000001",
        "1970-01-01 00:00:00.111111111",
        "1970-01-01 00:00:01.00000001",
        "9999-01-01 00:00:00.1"
    };

    String table = "test_arrow_ts_ntz";

    String column = "(a timestamp_ntz)";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while(i < cases.length)
    {
      rs.next();
      assertEquals(cases[i++], rs.getTimestamp(1).toString());
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampLTZ() throws SQLException, ParseException
  {
    for(int scale = 0; scale <= 9; scale++)
    {
      testTimestampLTZWithScale(scale);
    }
  }

  public void testTimestampLTZWithScale(int scale) throws SQLException, ParseException
  {
    String[] cases = {
        "2017-01-01 12:00:00 Z",
        "2014-01-02 16:00:00 Z",
        "2014-01-02 12:34:56 Z",
        "1970-01-01 00:00:00 Z",
        "1970-01-01 00:00:01 Z",
        "1969-12-31 11:59:59 Z",
        "0000-01-01 00:00:01 Z",
        "0001-12-31 11:59:59 Z"
    };

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(TimeZone.getDefault());

    String table = "test_arrow_ts_ltz";

    String column = "(a timestamp_ltz("+scale+"))";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while(i < cases.length)
    {
      rs.next();
      assertEquals(new Timestamp(dateFormat.parse(cases[i++]).getTime()), rs.getTimestamp(1));
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampLTZWithNanos() throws SQLException, ParseException
  {
    String[] cases = {
        "2017-01-01 12:00:00.123456789",
        "2014-01-02 16:00:00.000000001",
        "2014-01-02 12:34:56.1",
        "1970-01-01 00:00:00.123412423",
        "1970-01-01 00:00:01.000001",
        "1969-12-31 11:59:59.001",
        "0001-12-31 11:59:59.11"
    };

    String table = "test_arrow_ts_ltz";

    String column = "(a timestamp_ltz)";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
      Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while(i < cases.length)
    {
      rs.next();
      assertEquals(cases[i++], rs.getTimestamp(1).toString());
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampTZ() throws SQLException, ParseException
  {
    for(int scale = 0; scale <= 9; scale++)
    {
      testTimestampTZWithScale(scale);
    }
  }

  public void testTimestampTZWithScale(int scale) throws SQLException, ParseException
  {
    String[] cases = {
        "2017-01-01 12:00:00 Z",
        "2014-01-02 16:00:00 Z",
        "2014-01-02 12:34:56 Z",
        "1970-01-01 00:00:00 Z",
        "1970-01-01 00:00:01 Z",
        "1969-12-31 11:59:59 Z",
        "0000-01-01 00:00:01 Z",
        "0001-12-31 11:59:59 Z"
    };

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz("+scale+"))";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while(i < cases.length)
    {
      rs.next();
      assertEquals(new Timestamp(dateFormat.parse(cases[i++]).getTime()),
                   rs.getTimestamp(1));
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampTZWithNanos() throws SQLException, ParseException
  {
    String[] cases = {
        "2017-01-01 12:00:00.1",
        "2014-01-02 16:00:00.123456789",
        "2014-01-02 12:34:56.999999999",
        "1970-01-01 00:00:00.000000001",
        "1970-01-01 00:00:01.0000001",
        "1969-12-31 11:59:59.134",
        "0001-12-31 11:59:59.234141",
        "0000-01-01 00:00:01.790870987"
    };

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz)";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while(i < cases.length)
    {
      rs.next();
      if (i == cases.length-1)
      {
        // TODO: This is a JDBC bug which happens in both arrow and json cases
        assertEquals("0001-01-01 00:00:01.790870987", rs.getTimestamp(1).toString());
        i++;
      }
      else
      {
        assertEquals(cases[i++], rs.getTimestamp(1).toString());
      }
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }
}
