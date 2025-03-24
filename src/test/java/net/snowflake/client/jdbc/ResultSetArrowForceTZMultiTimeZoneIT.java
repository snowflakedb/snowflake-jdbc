package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Compare json and arrow resultSet behaviors 2/2 */
@Tag(TestTags.ARROW)
public class ResultSetArrowForceTZMultiTimeZoneIT extends ResultSetArrowForce0MultiTimeZone {

  @ParameterizedTest
  @ArgumentsSource(DataWithScaleProvider.class)
  public void testTimestampTZWithScale(String queryResultFormat, String tz, int scale)
      throws SQLException {
    setTimezone(tz);
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

    long[] times = {
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
    try (Connection con = init(table, column, values, queryResultFormat);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        int i = 0;
        while (i < cases.length) {
          assertTrue(rs.next());
          assertEquals(times[i++], rs.getTimestamp(1).getTime());
          assertEquals(0, rs.getTimestamp(1).getNanos());
        }
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      } finally {
        statement.execute("drop table " + table);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testTimestampTZWithNanos(String queryResultFormat, String tz) throws SQLException {
    setTimezone(tz);
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

    int[] nanos = {100000000, 123456789, 999999999, 1, 1, 100, 134000000, 234141000, 790870987};

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz)";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
    try (Connection con = init(table, column, values, queryResultFormat);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        int i = 0;
        while (i < cases.length) {
          assertTrue(rs.next());
          if (i == cases.length - 1 && tz.equalsIgnoreCase("utc")) {
            // TODO: Is this a JDBC bug which happens in both arrow and json cases?
            assertEquals("0001-01-01 00:00:01.790870987", rs.getTimestamp(1).toString());
          }

          assertEquals(times[i], rs.getTimestamp(1).getTime());
          assertEquals(nanos[i++], rs.getTimestamp(1).getNanos());
        }
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      } finally {
        statement.execute("drop table " + table);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testTimestampTZWithMicros(String queryResultFormat, String tz) throws SQLException {
    setTimezone(tz);
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
      100000000, 123456000, 999999000, 1000, 1000, 10000, 134000000, 234141000, 790870000
    };

    String table = "test_arrow_ts_tz";

    String column = "(a timestamp_tz(6))";

    String values = "('" + StringUtils.join(cases, " Z'),('") + " Z'), (null)";
    try (Connection con = init(table, column, values, queryResultFormat);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        int i = 0;
        while (i < cases.length) {
          assertTrue(rs.next());
          if (i == cases.length - 1 && tz.equalsIgnoreCase("utc")) {
            // TODO: Is this a JDBC bug which happens in both arrow and json cases?
            assertEquals("0001-01-01 00:00:01.79087", rs.getTimestamp(1).toString());
          }

          assertEquals(times[i], rs.getTimestamp(1).getTime());
          assertEquals(nanos[i++], rs.getTimestamp(1).getNanos());
        }
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      } finally {
        statement.execute("drop table " + table);
      }
    }
  }
}
