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
}
