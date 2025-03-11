package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.ProvidersUtil;
import net.snowflake.client.providers.ScaleProvider;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import net.snowflake.client.providers.SnowflakeArgumentsProvider;
import net.snowflake.client.providers.TimezoneProvider;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Completely compare json and arrow resultSet behaviors */
@Tag(TestTags.ARROW)
public class ResultSetJsonVsArrowMultiTZIT extends BaseJDBCWithSharedConnectionIT {
  static String originalTz;

  static class DataProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return ProvidersUtil.cartesianProduct(
          context, new SimpleResultFormatProvider(), new TimezoneProvider(3));
    }
  }

  static class DataWithScaleProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return ProvidersUtil.cartesianProduct(context, new DataProvider(), new ScaleProvider());
    }
  }

  @BeforeEach
  public void setSessionTimezone() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "alter session set "
              + "TIMEZONE='America/Los_Angeles',"
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    }
  }

  private static void setTimezone(String tz) {
    System.setProperty("user.timezone", tz);
  }

  @BeforeAll
  public static void saveTimezone() {
    originalTz = System.getProperty("user.timezone");
  }

  @AfterAll
  public static void restoreTimezone() {
    if (originalTz != null) {
      System.setProperty("user.timezone", originalTz);
    } else {
      System.clearProperty("user.timezone");
    }
  }

  private void init(String table, String column, String values, String queryResultFormat)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
      statement.execute("create or replace table " + table + " " + column);
      statement.execute("insert into " + table + " values " + values);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataWithScaleProvider.class)
  public void testTime(String queryResultFormat, String tz, int scale) throws SQLException {
    setTimezone(tz);
    String[] times = {
      "00:01:23",
      "00:01:23.1",
      "00:01:23.12",
      "00:01:23.123",
      "00:01:23.1234",
      "00:01:23.12345",
      "00:01:23.123456",
      "00:01:23.1234567",
      "00:01:23.12345678",
      "00:01:23.123456789"
    };
    testTimeWithScale(times, scale, queryResultFormat);
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testDate(String queryResultFormat, String tz) throws Exception {
    setTimezone(tz);
    String[] cases = {
      "2017-01-01",
      "2014-01-02",
      "2014-01-02",
      "1970-01-01",
      "1970-01-01",
      "1969-12-31",
      "0200-02-27",
      "0200-02-28",
      "0000-01-01",
      "0001-12-31"
    };
    String table = "test_arrow_date";

    String column = "(a date)";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    init(table, column, values, queryResultFormat);
    try (Statement statement = createStatement(queryResultFormat)) {
      try (ResultSet rs = statement.executeQuery("select * from " + table)) {
        int i = 0;
        while (i < cases.length) {
          assertTrue(rs.next());
          if (i == cases.length - 2) {
            assertEquals("0001-01-01", rs.getDate(1).toString());
          } else {
            assertEquals(cases[i], rs.getDate(1).toString());
          }
          i++;
        }
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      }
      statement.execute("drop table " + table);
      System.clearProperty("user.timezone");
    }
  }

  public void testTimeWithScale(String[] times, int scale, String queryResultFormat)
      throws SQLException {
    String table = "test_arrow_time";
    String column = "(a time(" + scale + "))";
    String values = "('" + StringUtils.join(times, "'),('") + "'), (null)";
    init(table, column, values, queryResultFormat);
    try (Statement statement = createStatement(queryResultFormat);
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      for (int i = 0; i < times.length; i++) {
        assertTrue(rs.next());
        // Java Time class does not have nanoseconds
        assertEquals("00:01:23", rs.getString(1));
      }
      assertTrue(rs.next());
      assertNull(rs.getTime(1));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataWithScaleProvider.class)
  public void testTimestampNTZWithScale(String queryResultFormat, String tz, int scale)
      throws SQLException {
    setTimezone(tz);
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

    String column = "(a timestamp_ntz(" + scale + "))";

    String values = "('" + StringUtils.join(cases, "'),('") + "'), (null)";
    init(table, column, values, queryResultFormat);
    try (Statement statement = createStatement(queryResultFormat)) {
      try (ResultSet rs = statement.executeQuery("select * from " + table)) {
        int i = 0;
        while (i < cases.length) {
          assertTrue(rs.next());
          assertEquals(results[i++], rs.getString(1));
        }
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      }
      statement.execute("drop table " + table);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testTimestampNTZWithNanos(String queryResultFormat, String tz) throws SQLException {
    setTimezone(tz);
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
    init(table, column, values, queryResultFormat);
    try (Statement statement = createStatement(queryResultFormat)) {
      try (ResultSet rs = statement.executeQuery("select * from " + table)) {
        int i = 0;
        while (i < cases.length) {
          assertTrue(rs.next());
          assertEquals(cases[i++], rs.getTimestamp(1).toString());
        }
        assertTrue(rs.next());
        assertNull(rs.getString(1));
      } finally {
        statement.execute("drop table " + table);
      }
    }
  }
}
