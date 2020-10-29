/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Compare json and arrow resultSet behaviors 2/2
 */
@RunWith(Parameterized.class)
@Category(TestCategoryArrow.class)
public class ResultSetArrowForceTZMultiTimeZoneIT extends ResultSetArrowForce0MultiTimeZone
{
  @Parameterized.Parameters(name = "format={0}, tz={1}")
  public static Object[][] data()
  {
    return ResultSetArrowForce0MultiTimeZone.testData();
  }

  public ResultSetArrowForceTZMultiTimeZoneIT(String queryResultFormat, String timeZone)
  {
    super(queryResultFormat, timeZone);
  }

  @Test
  public void testTimestampTZ() throws SQLException
  {
    for (int scale = 0; scale <= 9; scale++)
    {
      testTimestampTZWithScale(scale);
    }
  }

  private void testTimestampTZWithScale(int scale) throws SQLException
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
            1483272000000L,
            1388678400000L,
            1388666096000L,
            0,
            1000,
            -43201000L,
            -62167391999000L,
            -62104276801000L
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
  public void testTimestampTZWithNanos() throws SQLException
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
        1483272000100L,
        1388678400123L,
        1388666096999L,
        -1000,
        0,
        1000,
        -43200866,
        -62104276800766L,
        -62167391998210L
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
  public void testTimestampTZWithMicros() throws SQLException
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
        1483272000100L,
        1388678400123L,
        1388666096999L,
        -1000,
        0,
        1000,
        -43200866,
        -62104276800766L,
        -62167391998210L
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
