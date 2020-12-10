package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * ResultSet multi timezone tests for the latest JDBC driver. This cannot run for the old driver.
 */
@RunWith(Parameterized.class)
@Category(TestCategoryResultSet.class)
public class ResultSetMultiTimeZoneLatestIT extends BaseJDBCTest {
  @Parameterized.Parameters(name = "format={0}, tz={1}")
  public static Collection<Object[]> data() {
    // all tests in this class need to run for both query result formats json and arrow
    String[] timeZones = new String[] {"UTC", "Asia/Singapore", "MEZ"};
    String[] queryFormats = new String[] {"json", "arrow"};
    List<Object[]> ret = new ArrayList<>();
    for (String queryFormat : queryFormats) {
      for (String timeZone : timeZones) {
        ret.add(new Object[] {queryFormat, timeZone});
      }
    }
    return ret;
  }

  private final String queryResultFormat;

  public ResultSetMultiTimeZoneLatestIT(String queryResultFormat, String timeZone) {
    this.queryResultFormat = queryResultFormat;
    System.setProperty("user.timezone", timeZone);
  }

  public Connection init() throws SQLException {
    Connection connection = BaseJDBCTest.getConnection();

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
    connection
        .createStatement()
        .execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    return connection;
  }

  /**
   * Test for getDate(int columnIndex, Calendar cal) function to ensure it matches values with
   * getTimestamp function
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDateAndTimestampWithTimezone() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("alter session set JDBC_FORMAT_DATE_WITH_TIMEZONE=true");
    ResultSet rs =
        statement.executeQuery(
            "SELECT DATE '1970-01-02 00:00:00' as datefield, "
                + "TIMESTAMP '1970-01-02 00:00:00' as timestampfield");
    rs.next();

    // Set a timezone for results to be returned in and set a format for date and timestamp objects
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(cal.getTimeZone());

    // Date object and calendar object should return the same timezone offset with calendar
    Date dateWithZone = rs.getDate(1, cal);
    Timestamp timestampWithZone = rs.getTimestamp(2, cal);
    assertEquals(sdf.format(dateWithZone), sdf.format(timestampWithZone));

    // When fetching Date object with getTimestamp versus Timestamp object with getTimestamp,
    // results should match
    assertEquals(rs.getTimestamp(1, cal), rs.getTimestamp(2, cal));

    // When fetching Timestamp object with getDate versus Date object with getDate, results should
    // match
    assertEquals(rs.getDate(1, cal), rs.getDate(2, cal));

    // getDate() without Calendar offset called on Date type should return the same date with no
    // timezone offset
    assertEquals("1970-01-02 00:00:00", sdf.format(rs.getDate(1)));
    // getDate() without Calendar offset called on Timestamp type returns date with timezone offset
    assertEquals("1970-01-02 08:00:00", sdf.format(rs.getDate(2)));

    // getTimestamp() without Calendar offset called on Timestamp type should return the timezone
    // offset
    assertEquals("1970-01-02 08:00:00", sdf.format(rs.getTimestamp(2)));
    // getTimestamp() without Calendar offset called on Date type should not return the timezone
    // offset
    assertEquals("1970-01-02 00:00:00", sdf.format(rs.getTimestamp(1)));

    // test that session parameter functions as expected. When false, getDate() has same behavior
    // with or without Calendar input
    statement.execute("alter session set JDBC_FORMAT_DATE_WITH_TIMEZONE=false");
    rs = statement.executeQuery("SELECT DATE '1945-05-10 00:00:00' as datefield");
    rs.next();
    assertEquals(rs.getDate(1, cal), rs.getDate(1));
    assertEquals("1945-05-10 00:00:00", sdf.format(rs.getDate(1, cal)));

    rs.close();
    statement.close();
    connection.close();
  }

  /**
   * Tests current behavior (previous to adding JDBC_USE_SESSION_TIMEZONE)
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSessionTimezoneSetToFalse() throws SQLException {
    helperTestUseSessionTimeZone(false, true);
  }

  /**
   * Tests that formats are correct when JDBC_USE_SESSION_TIMEZONE=true
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSessionTimezoneUsage() throws SQLException {
    helperTestUseSessionTimeZone(true, true);
  }

  /**
   * Tests that the new param overrides previous time/date/timestamp formatting parameters such as
   * JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC, CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ, and
   * JDBC_FORMAT_DATE_WITH_TIMEZONE.
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testUseSessionTimeZoneOverrides() throws SQLException {
    helperTestUseSessionTimeZone(true, false);
  }

  private void helperTestUseSessionTimeZone(
      boolean useSessionTimezone, boolean useDefaultParamSettings) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    // create table with all timestamp types, time, and date
    statement.execute(
        "create or replace table datetimetypes(colA timestamp_ltz, colB timestamp_ntz, colC timestamp_tz, colD time, colE date)");
    if (useSessionTimezone) {
      statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=true");
    } else {
      statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=false");
    }
    if (!useDefaultParamSettings) {
      // these are 3 other session params that also alter the session display behavior
      statement.execute("alter session set JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=true");
      statement.execute("alter session set CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ=false");
      statement.execute("alter session set JDBC_FORMAT_DATE_WITH_TIMEZONE=true");
    }

    String expectedTimestamp = "2019-01-01 17:17:17.6";
    String expectedTime = "17:17:17";
    String expectedDate = "2019-01-01";
    String expectedTimestamp2 = "1943-12-31 01:01:33.0";
    String expectedTime2 = "01:01:33";
    String expectedDate2 = "1943-12-31";
    PreparedStatement prepSt =
        connection.prepareStatement("insert into datetimetypes values(?, ?, ?, ?, ?)");
    prepSt.setString(1, expectedTimestamp);
    prepSt.setString(2, expectedTimestamp);
    prepSt.setString(3, expectedTimestamp);
    prepSt.setString(4, expectedTime);
    prepSt.setString(5, expectedDate);
    prepSt.execute();
    prepSt.setString(1, expectedTimestamp2);
    prepSt.setString(2, expectedTimestamp2);
    prepSt.setString(3, expectedTimestamp2);
    prepSt.setString(4, expectedTime2);
    prepSt.setString(5, expectedDate2);
    prepSt.execute();

    // Results differ depending on whether flag JDBC_USE_SESSION_TIMEZONE=true. If true, the
    // returned ResultSet value should match the value inserted into the table with no offset (with
    // exceptions for getTimestamp() on date and time objects).
    ResultSet rs = statement.executeQuery("select * from datetimetypes");
    rs.next();
    // Assert date has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(useSessionTimezone, rs.getDate("COLA").toString().equals(expectedDate));
    // always true since timezone_ntz doesn't add time offset
    assertEquals(true, rs.getDate("COLB").toString().equals(expectedDate));
    assertEquals(useSessionTimezone, rs.getDate("COLC").toString().equals(expectedDate));
    // cannot getDate() for Time column (ColD)
    // always true since Date objects don't have timezone offsets
    assertEquals(true, rs.getDate("COLE").toString().equals(expectedDate));

    // Assert timestamp has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(useSessionTimezone, rs.getTimestamp("COLA").toString().equals(expectedTimestamp));
    // always true since timezone_ntz doesn't add time offset
    assertEquals(true, rs.getTimestamp("COLB").toString().equals(expectedTimestamp));
    assertEquals(useSessionTimezone, rs.getTimestamp("COLC").toString().equals(expectedTimestamp));
    // Getting timestamp from Time column will default to epoch start date
    assertEquals(true, rs.getTimestamp("COLD").toString().equals("1970-01-01 17:17:17.0"));
    // Getting timestamp from Date column will default to wallclock time of 0
    assertEquals(true, rs.getTimestamp("COLE").toString().equals("2019-01-01 00:00:00.0"));

    // Assert time has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(useSessionTimezone, rs.getTime("COLA").toString().equals(expectedTime));
    assertEquals(true, rs.getTime("COLB").toString().equals(expectedTime));
    assertEquals(useSessionTimezone, rs.getTime("COLC").toString().equals(expectedTime));
    assertEquals(true, rs.getTime("COLD").toString().equals(expectedTime));
    // Cannot getTime() for Date column (colE)

    rs.next();
    // Assert date has no offset. Offset will never be seen regardless of flag because 01:01:33 is
    // too early for any timezone to round it to the next day.
    assertEquals(true, rs.getDate("COLA").toString().equals(expectedDate2));
    assertEquals(true, rs.getDate("COLB").toString().equals(expectedDate2));
    assertEquals(true, rs.getDate("COLC").toString().equals(expectedDate2));
    // cannot getDate() for Time column (ColD)
    assertEquals(true, rs.getDate("COLE").toString().equals(expectedDate2));

    // Assert timestamp has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(useSessionTimezone, rs.getTimestamp("COLA").toString().equals(expectedTimestamp2));
    assertEquals(true, rs.getTimestamp("COLB").toString().equals(expectedTimestamp2));
    assertEquals(useSessionTimezone, rs.getTimestamp("COLC").toString().equals(expectedTimestamp2));
    // Getting timestamp from Time column will default to epoch start date
    assertEquals(true, rs.getTimestamp("COLD").toString().equals("1970-01-01 01:01:33.0"));
    // Getting timestamp from Date column will default to wallclock time of 0
    assertEquals(true, rs.getTimestamp("COLE").toString().equals("1943-12-31 00:00:00.0"));

    // Assert time has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(useSessionTimezone, rs.getTime("COLA").toString().equals(expectedTime2));
    assertEquals(true, rs.getTime("COLB").toString().equals(expectedTime2));
    assertEquals(useSessionTimezone, rs.getTime("COLC").toString().equals(expectedTime2));
    assertEquals(true, rs.getTime("COLD").toString().equals(expectedTime2));
    // Cannot getTime() for Date column (colE)

    // clean up
    statement.execute("alter session unset JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC");
    statement.execute("alter session unset CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ");
    statement.execute("alter session unset JDBC_FORMAT_DATE_WITH_TIMEZONE");
    statement.execute("alter session unset JDBC_USE_SESSION_TIMEZONE");

    rs.close();
    statement.close();
    connection.close();
  }
}
