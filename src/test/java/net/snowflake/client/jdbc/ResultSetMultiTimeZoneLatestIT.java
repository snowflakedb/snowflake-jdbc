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
    // UTC and Europe/London have different offsets during daylight savings time so it is important
    // to test both to ensure daylight savings time is correct
    String[] timeZones = new String[] {"UTC", "Asia/Singapore", "MEZ", "Europe/London"};
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
   * This tests that all time values (regardless of precision) return the same wallclock value when
   * getTimestamp() is called.
   *
   * @throws SQLException
   */
  @Test
  public void testTimesWithGetTimestamp() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    String timeStringValue = "10:30:50.123456789";
    String timestampStringValue = "1970-01-01 " + timeStringValue;
    int length = timestampStringValue.length();
    statement.execute(
        "create or replace table SRC_DATE_TIME (C2_TIME_3 TIME(3), C3_TIME_5 TIME(5), C4_TIME"
            + " TIME(9))");
    statement.execute(
        "insert into SRC_DATE_TIME values ('"
            + timeStringValue
            + "','"
            + timeStringValue
            + "','"
            + timeStringValue
            + "')");
    ResultSet rs = statement.executeQuery("select * from SRC_DATE_TIME");
    rs.next();
    assertEquals(timestampStringValue.substring(0, length - 6), rs.getTimestamp(1).toString());
    assertEquals(timestampStringValue.substring(0, length - 4), rs.getTimestamp(2).toString());
    assertEquals(timestampStringValue, rs.getTimestamp(3).toString());
  }

  /**
   * This test is for SNOW-366563 where the timestamp was returning an incorrect value for daylight
   * savings time due to the fact that UTC and Europe/London time have the same offset until
   * daylight savings comes into effect. This tests that the timestamp value is accurate during
   * daylight savings time.
   *
   * @throws SQLException
   */
  @Test
  public void testTimestampNTZWithDaylightSavings() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute(
        "alter session set TIMESTAMP_TYPE_MAPPING='TIMESTAMP_NTZ'," + "TIMEZONE='Europe/London'");
    ResultSet rs = statement.executeQuery("select TIMESTAMP '2011-09-04 00:00:00'");
    rs.next();
    Timestamp expected = Timestamp.valueOf("2011-09-04 00:00:00");
    assertEquals(expected, rs.getTimestamp(1));
    rs.close();
    statement.close();
    connection.close();
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
   * Tests that formats are correct when JDBC_USE_SESSION_TIMEZONE=true and other related time/date
   * formatting parameters are at their default values
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSessionTimezoneUsage() throws SQLException {
    testUseSessionTimeZoneHelper(true);
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
    testUseSessionTimeZoneHelper(false);
  }

  /**
   * Helper function to test behavior of parameter JDBC_USE_SESSION_TIMEZONE. When
   * JDBC_USE_SESSION_TIMEZONE=true, time/date/timestamp values are displayed using the session
   * timezone, not the JVM timezone. There should be no offset between the inserted value and the
   * displayed value from ResultSet. For example, a timestamp value inserted as: 2019-01-01
   * 17:17:17.6 +0500 will get displayed as so: ResultSet.getTimestamp(): 2019-01-01 17:17:17.6;
   * ResultSet.getTime(): 17:17:17.6; ResultSet.getDate(): 2019-01-01
   *
   * <p>When JDBC_USE_SESSION_TIMEZONE=false, the displayed values will be different depending on
   * the timezone offset between the session and JVM timezones.
   *
   * @param useDefaultParamSettings use default settings of other time/date session formatting
   *     parameters
   * @throws SQLException
   */
  private void testUseSessionTimeZoneHelper(boolean useDefaultParamSettings) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    // create table with all timestamp types, time, and date
    statement.execute(
        "create or replace table datetimetypes(colA timestamp_ltz, colB timestamp_ntz, colC"
            + " timestamp_tz, colD time, colE date)");
    // Enable session parameter JDBC_USE_SESSION_TIMEZONE
    statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=true");
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
    assertEquals(expectedDate, rs.getDate("COLA").toString());
    // always true since timezone_ntz doesn't add time offset
    assertEquals(expectedDate, rs.getDate("COLB").toString());
    assertEquals(expectedDate, rs.getDate("COLC").toString());
    // cannot getDate() for Time column (ColD)
    // always true since Date objects don't have timezone offsets
    assertEquals(expectedDate, rs.getDate("COLE").toString());

    // Assert timestamp has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(expectedTimestamp, rs.getTimestamp("COLA").toString());
    // always true since timezone_ntz doesn't add time offset
    assertEquals(expectedTimestamp, rs.getTimestamp("COLB").toString());
    assertEquals(expectedTimestamp, rs.getTimestamp("COLC").toString());
    // Getting timestamp from Time column will default to epoch start date so date portion is
    // different than input date of the timestamp
    assertEquals("1970-01-01 17:17:17.0", rs.getTimestamp("COLD").toString());
    // Getting timestamp from Date column will default to wallclock time of 0 so time portion is
    // different than input time of the timestamp
    assertEquals("2019-01-01 00:00:00.0", rs.getTimestamp("COLE").toString());

    // Assert time has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(expectedTime, rs.getTime("COLA").toString());
    assertEquals(expectedTime, rs.getTime("COLB").toString());
    assertEquals(expectedTime, rs.getTime("COLC").toString());
    assertEquals(expectedTime, rs.getTime("COLD").toString());
    // Cannot getTime() for Date column (colE)

    rs.next();
    // Assert date has no offset. Offset will never be seen regardless of flag because 01:01:33 is
    // too early for any timezone to round it to the next day.
    assertEquals(expectedDate2, rs.getDate("COLA").toString());
    assertEquals(expectedDate2, rs.getDate("COLB").toString());
    assertEquals(expectedDate2, rs.getDate("COLC").toString());
    // cannot getDate() for Time column (ColD)
    assertEquals(expectedDate2, rs.getDate("COLE").toString());

    // Assert timestamp has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(expectedTimestamp2, rs.getTimestamp("COLA").toString());
    assertEquals(expectedTimestamp2, rs.getTimestamp("COLB").toString());
    assertEquals(expectedTimestamp2, rs.getTimestamp("COLC").toString());
    // Getting timestamp from Time column will default to epoch start date
    assertEquals("1970-01-01 01:01:33.0", rs.getTimestamp("COLD").toString());
    // Getting timestamp from Date column will default to wallclock time of 0
    assertEquals("1943-12-31 00:00:00.0", rs.getTimestamp("COLE").toString());

    // Assert time has no offset. When flag is false, timestamp_ltz and timestamp_ntz will show
    // offset.
    assertEquals(expectedTime2, rs.getTime("COLA").toString());
    assertEquals(expectedTime2, rs.getTime("COLB").toString());
    assertEquals(expectedTime2, rs.getTime("COLC").toString());
    assertEquals(expectedTime2, rs.getTime("COLD").toString());
    // Cannot getTime() for Date column (colE)

    // Test special case for timestamp_tz (offset added)
    // create table with of type timestamp_tz
    statement.execute("create or replace table tabletz (colA timestamp_tz)");
    prepSt = connection.prepareStatement("insert into tabletz values(?), (?)");
    // insert 2 timestamp values, but add an offset of a few hours on the end of each value
    prepSt.setString(
        1, expectedTimestamp + " +0500"); // inserted value is 2019-01-01 17:17:17.6 +0500
    prepSt.setString(
        2, expectedTimestamp2 + " -0200"); // inserted value is 1943-12-31 01:01:33.0 -0200
    prepSt.execute();

    rs = statement.executeQuery("select * from tabletz");
    rs.next();
    // Assert timestamp is displayed with no offset when flag is true. Timestamp should look
    // identical to inserted value
    assertEquals(expectedTimestamp, rs.getTimestamp("COLA").toString());
    // Time value looks identical to the time portion of inserted timestamp_tz value
    assertEquals(expectedTime, rs.getTime("COLA").toString());
    // Date value looks identical to the date portion of inserted timestamp_tz value
    assertEquals(expectedDate, rs.getDate("COLA").toString());
    rs.next();
    // Test that the same results occur for 2nd timestamp_tz value
    assertEquals(expectedTimestamp2, rs.getTimestamp("COLA").toString());
    assertEquals(expectedTime2, rs.getTime("COLA").toString());
    assertEquals(expectedDate2, rs.getDate("COLA").toString());

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
