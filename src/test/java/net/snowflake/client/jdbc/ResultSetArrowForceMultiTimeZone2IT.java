package net.snowflake.client.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Completely compare json and arrow resultSet behaviors
 */
@RunWith(Parameterized.class)
public class ResultSetArrowForceMultiTimeZone2IT extends BaseJDBCTest
{
  @Parameterized.Parameters(name = "format={0}, tz={1}")
  public static Object[][] data()
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

  private static String queryResultFormat;
  private String tz;

  public static Connection getConnection(int injectSocketTimeout)
  throws SQLException
  {
    Connection connection = BaseJDBCTest.getConnection(injectSocketTimeout);

    Statement statement = connection.createStatement();
    statement.execute(
        "alter session set " +
        "TIMEZONE='America/Los_Angeles'," +
        "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ'," +
        "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
        "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
        "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'," +
        "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    statement.close();
    return connection;
  }

  public ResultSetArrowForceMultiTimeZone2IT(String queryResultFormat, String timeZone)
  {
    this.queryResultFormat = queryResultFormat;
    System.setProperty("user.timezone", timeZone);
    tz = timeZone;
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
    conn.createStatement().execute("alter session set query_result_format = '"
                                       + queryResultFormat + "'");
    return conn;
  }

  @Test
  public void testTimestampLTZ() throws SQLException, ParseException
  {
    for (int scale = 0; scale <= 9; scale++)
    {
      testTimestampLTZWithScale(scale);
    }
  }

  @Test
  public void testTimestampLTZOutputFormat() throws SQLException
  {
    String[] cases = {
        "2017-01-01 12:00:00 Z",
        "2014-01-02 16:00:00 Z",
        "2014-01-02 12:34:56 Z"
    };

    long[] times =
        {
            1483272000000l,
            1388678400000l,
            1388666096000l
        };

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(TimeZone.getDefault());

    String table = "test_arrow_ts_ltz";

    String column = "(a timestamp_ltz)";

    String values = "('" + StringUtils.join(cases, "'),('") + "')";
    Connection con = init(table, column, values);

    Statement statement = con.createStatement();

    // use initialized ltz output format
    ResultSet rs = statement.executeQuery("select * from " + table);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(times[i], rs.getTimestamp(1).getTime());
      String weekday = rs.getString(1).split(",")[0];
      assertEquals(3, weekday.length());
    }


    // change ltz output format
    statement.execute("alter session set TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS TZH:TZM'");
    rs = statement.executeQuery("select * from " + table);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(times[i], rs.getTimestamp(1).getTime());
      String year = rs.getString(1).split("-")[0];
      assertEquals(4, year.length());
    }

    // unset ltz output format, then it should use timestamp_output_format
    statement.execute("alter session unset TIMESTAMP_LTZ_OUTPUT_FORMAT");
    rs = statement.executeQuery("select * from " + table);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(times[i], rs.getTimestamp(1).getTime());
      String weekday = rs.getString(1).split(",")[0];
      assertEquals(3, weekday.length());
    }

    // set ltz output format back to init value
    statement.execute("alter session set TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    rs = statement.executeQuery("select * from " + table);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(times[i], rs.getTimestamp(1).getTime());
      String weekday = rs.getString(1).split(",")[0];
      assertEquals(3, weekday.length());
    }

    finish(table, con);
  }

  @Test
  public void testTimestampLTZWithNulls() throws SQLException
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

    long[] times =
        {
            1483272000000l,
            1388678400000l,
            1388666096000l,
            0,
            1000,
            -43201000,
            -62167391999000l,
            -62104276801000l
        };

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(TimeZone.getDefault());

    String table = "test_arrow_ts_ltz";

    String column = "(a timestamp_ltz)";

    String values = "('" + StringUtils.join(cases, "'), (null),('") + "')";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while (i < 2 * cases.length - 1)
    {
      rs.next();
      if (i % 2 != 0)
      {
        assertNull(rs.getTimestamp(1));
      }
      else
      {
        assertEquals(times[i / 2], rs.getTimestamp(1).getTime());
        assertEquals(0, rs.getTimestamp(1).getNanos());
      }
      i++;
    }
    finish(table, con);
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

    long[] times =
        {
            1483272000000l,
            1388678400000l,
            1388666096000l,
            0,
            1000,
            -43201000,
            -62167391999000l,
            -62104276801000l
        };

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(TimeZone.getDefault());

    String table = "test_arrow_ts_ltz";

    String column = "(a timestamp_ltz(" + scale + "))";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while (i < cases.length)
    {
      rs.next();
      assertEquals(times[i++], rs.getTimestamp(1).getTime());
      assertEquals(0, rs.getTimestamp(1).getNanos());
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampLTZWithNanos() throws SQLException, ParseException
  {
    String[] cases =
        {
            "2017-01-01 12:00:00.123456789",
            "2014-01-02 16:00:00.000000001",
            "2014-01-02 12:34:56.1",
            "1969-12-31 23:59:59.000000001",
            "1970-01-01 00:00:00.123412423",
            "1970-01-01 00:00:01.000001",
            "1969-12-31 11:59:59.001",
            "0001-12-31 11:59:59.11"
        };

    long[] times =
        {
            1483272000123l,
            1388678400000l,
            1388666096100l,
            -1000,
            123,
            1000,
            -43200999,
            -62104276800890l
        };

    int[] nanos =
        {
            123456789,
            1,
            100000000,
            1,
            123412423,
            1000,
            1000000,
            110000000
        };

    String table = "test_arrow_ts_ltz";

    String column = "(a timestamp_ltz)";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while (i < cases.length)
    {
      rs.next();
      assertEquals(times[i], rs.getTimestamp(1).getTime());
      assertEquals(nanos[i++], rs.getTimestamp(1).getNanos());
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampTZ() throws SQLException, ParseException
  {
    for (int scale = 0; scale <= 9; scale++)
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

    long[] times =
        {
            1483272000000l,
            1388678400000l,
            1388666096000l,
            0,
            1000,
            -43201000l,
            -62167391999000l,
            -62104276801000l
        };

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz(" + scale + "))";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while (i < cases.length)
    {
      rs.next();
      assertEquals(times[i++], rs.getTimestamp(1).getTime());
      assertEquals(0, rs.getTimestamp(1).getNanos());

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
        "1969-12-31 23:59:59.000000001",
        "1970-01-01 00:00:00.000000001",
        "1970-01-01 00:00:01.0000001",
        "1969-12-31 11:59:59.134",
        "0001-12-31 11:59:59.234141",
        "0000-01-01 00:00:01.790870987"
    };

    long[] times = {
        1483272000100l,
        1388678400123l,
        1388666096999l,
        -1000,
        0,
        1000,
        -43200866,
        -62104276800766l,
        -62167391998210l
    };

    int[] nanos = {
        100000000,
        123456789,
        999999999,
        1,
        1,
        100,
        134000000,
        234141000,
        790870987
    };

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz)";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while (i < cases.length)
    {
      rs.next();
      if (i == cases.length - 1 && tz.equalsIgnoreCase("utc"))
      {
        // TODO: Is this a JDBC bug which happens in both arrow and json cases?
        assertEquals("0001-01-01 00:00:01.790870987", rs.getTimestamp(1).toString());
      }

      assertEquals(times[i], rs.getTimestamp(1).getTime());
      assertEquals(nanos[i++], rs.getTimestamp(1).getNanos());
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }

  @Test
  public void testTimestampTZWithMicros() throws SQLException, ParseException
  {
    String[] cases = {
        "2017-01-01 12:00:00.1",
        "2014-01-02 16:00:00.123456",
        "2014-01-02 12:34:56.999999",
        "1969-12-31 23:59:59.000001",
        "1970-01-01 00:00:00.000001",
        "1970-01-01 00:00:01.00001",
        "1969-12-31 11:59:59.134",
        "0001-12-31 11:59:59.234141",
        "0000-01-01 00:00:01.79087"
    };

    long[] times = {
        1483272000100l,
        1388678400123l,
        1388666096999l,
        -1000,
        0,
        1000,
        -43200866,
        -62104276800766l,
        -62167391998210l
    };

    int[] nanos = {
        100000000,
        123456000,
        999999000,
        1000,
        1000,
        10000,
        134000000,
        234141000,
        790870000
    };

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz(6))";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    while (i < cases.length)
    {
      rs.next();
      if (i == cases.length - 1 && tz.equalsIgnoreCase("utc"))
      {
        // TODO: Is this a JDBC bug which happens in both arrow and json cases?
        assertEquals("0001-01-01 00:00:01.79087", rs.getTimestamp(1).toString());
      }

      assertEquals(times[i], rs.getTimestamp(1).getTime());
      assertEquals(nanos[i++], rs.getTimestamp(1).getNanos());
    }
    rs.next();
    assertNull(rs.getString(1));
    finish(table, con);
  }
}
