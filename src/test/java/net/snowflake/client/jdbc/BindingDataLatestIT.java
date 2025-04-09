package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.zone.ZoneRules;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ResultUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Binding Data integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the version of oldest supported driver
 * to examine if the tests still are not applicable. If it is applicable, move tests to
 * BindingDataIT so that both the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.OTHERS)
public class BindingDataLatestIT extends AbstractDriverIT {
  TimeZone origTz = TimeZone.getDefault();
  TimeZone tokyoTz = TimeZone.getTimeZone("Asia/Tokyo");
  TimeZone australiaTz = TimeZone.getTimeZone("Australia/Sydney");
  Calendar tokyo = Calendar.getInstance(tokyoTz);

  public static long[] removeLastN(long[] originalArray, int n) {
    if (originalArray == null) {
      return null; // Or throw IllegalArgumentException
    }
    if (n < 0) {
      throw new IllegalArgumentException("Number of elements to remove cannot be negative.");
    }

    int originalLength = originalArray.length;

    // Calculate the length of the new array
    // If original length is less than or equal to n, the new array will be empty.
    int newLength = Math.max(0, originalLength - n);

    // Create a new array with the calculated length and copy elements
    long[] newArray = Arrays.copyOf(originalArray, newLength);

    return newArray;
  }

  @Test
  public void testTimezone() throws NoSuchFieldException, IllegalAccessException {
    TimeZone timeZone = TimeZone.getTimeZone("America/Chicago");
    TimeZone.setDefault(timeZone);

    // Bind uploader constructor
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    dateFormat.setCalendar(calendar);

    Date date1 = generateDateWithCustomerCode(timeZone);

    // SnowflakePreparedStatementV1 setDate
    int offset = timeZone.getOffset(date1.getTime());
    long msDiffJulianToGregorian = ResultUtil.msDiffJulianToGregorian(date1);
    String value = String.valueOf(date1.getTime() + offset - msDiffJulianToGregorian);
    Long parsed = Long.parseLong(value);
    java.sql.Date date = new java.sql.Date(parsed);
    String formattedWithDefaultTimeZone = dateFormat.format(date);

    /// Overwrite transition field in time zone
//    Class<?> timezoneClass = timeZone.getClass();
//    Field transitionsField = timezoneClass.getDeclaredField("transitions");
//    transitionsField.setAccessible(true);
//    long[] transitions = (long[]) transitionsField.get(timeZone);
//    long[] result = removeLastN(transitions, 361 - 253);
//    transitionsField.set(timeZone, result);
    /// End

    Calendar calendar2 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
    dateFormat2.setCalendar(calendar2);

    Date date2 = generateDateWithCustomerCode(timeZone);

    int offset2 = timeZone.getOffset(date2.getTime());
    long msDiffJulianToGregorian2 = ResultUtil.msDiffJulianToGregorian(date2);
    String value2 = String.valueOf(date2.getTime() + offset2 - msDiffJulianToGregorian2);
    Long parsed2 = Long.parseLong(value2);
    java.sql.Date date22 = new java.sql.Date(parsed2);
    String formattedWithTruncatedTimeZone = dateFormat2.format(date22);

    assertEquals(formattedWithDefaultTimeZone, formattedWithTruncatedTimeZone);
  }

  private static Date generateDateWithCustomerCode(TimeZone timeZone) {
    Calendar cal = Calendar.getInstance(timeZone, Locale.US);
    cal.clear();
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(Calendar.YEAR, 2016);
    cal.set(Calendar.MONTH, 2 - 1);
    cal.set(Calendar.DATE, 19);
    Date date1 = new Date(cal.getTimeInMillis());
    return date1;
  }

  @Test
  public void testBinding() throws SQLException, NoSuchFieldException, IllegalAccessException {
    TimeZone timeZone = TimeZone.getTimeZone("America/Chicago");
    TimeZone.setDefault(timeZone);
    Locale.setDefault(Locale.US);
    Properties props = new Properties();
    try (Connection connection = getConnection(DONT_INJECT_SOCKET_TIMEOUT, props, false, false);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table stageinsertdates(ind int, d1 date )");
        statement.execute("create or replace table regularinsertdates(ind int, d1 date)");
        //        statement.execute("alter session set JDBC_FORMAT_DATE_WITH_TIMEZONE=false");
        statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=false");
        //        statement.execute("alter session set JDBC_QUERY_RESULT_FORMAT=false");

        Calendar cal = Calendar.getInstance(TimeZone.getDefault(), Locale.US);
        // Then the calendar is set to a year, month, date value from a “DateDataWrapper ddw” which
        // is just a class that holds the int year, month (1-based) and day values sent from the
        // source:
        cal.clear();
        int year = 2016;
        if (year <= 0) {
          cal.set(Calendar.ERA, GregorianCalendar.BC);
        } else {
          cal.set(Calendar.ERA, GregorianCalendar.AD);
        }
        cal.set(Calendar.YEAR, Math.abs(year));
        cal.set(Calendar.MONTH, 2 - 1);
        cal.set(Calendar.DATE, 19);
        Date date1 = new java.sql.Date(cal.getTimeInMillis());
        System.out.println(cal.getTimeInMillis());
        System.out.println(date1);

        // insert using regular binging
        executePrepStmtForNumRowsDates(connection, "regularinsertdates", date1, 3);

        // insert using stage binding
        //        statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
        executePrepStmtForNumRowsDates(connection, "stageinsertdates", date1, 65280 + 1);
        try (ResultSet rs1 = statement.executeQuery("select * from stageinsertdates");
            ResultSet rs2 = statement.executeQuery("select * from regularinsertdates")) {
          for (int i = 0; i < 3; i++) {
            assertTrue(rs1.next());
            assertTrue(rs2.next());

            assertEquals(rs1.getInt(1), rs2.getInt(1));

            assertEquals("X", rs1.getString(2));
            assertEquals("X", rs1.getDate(2));
            assertEquals("X", rs2.getString(2));
            assertEquals("X", rs2.getDate(2));
          }
        }
      } finally {
        TimeZone.setDefault(origTz);
      }
    }
  }

  private void executePrepStmtForNumRowsDates(
      Connection connection, String tableName, Date date1, int numRows) throws SQLException {
    try (PreparedStatement prepStatement =
        connection.prepareStatement("insert into " + tableName + " values (?,?)")) {
      for (int i = 0; i < numRows; i++) {
        prepStatement.setInt(1, 1);
        prepStatement.setDate(2, date1);
        prepStatement.addBatch();
      }
      prepStatement.executeBatch();
      prepStatement.getConnection().commit();
    }
  }

  @Test
  public void testBindTimestampTZ() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create or replace table testBindTimestampTZ(cola int, colb timestamp_tz)");
      statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_TZ");

      long milliSeconds = System.currentTimeMillis();
      Timestamp ts = new Timestamp(milliSeconds);
      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into testBindTimestampTZ values (?, ?)")) {
        prepStatement.setInt(1, 123);
        prepStatement.setTimestamp(2, ts, Calendar.getInstance(TimeZone.getTimeZone("EST")));
        prepStatement.execute();
      }

      try (ResultSet resultSet =
          statement.executeQuery("select cola, colb from testBindTimestampTz")) {
        assertTrue(resultSet.next());
        assertThat("integer", resultSet.getInt(1), equalTo(123));
        assertThat("timestamp_tz", resultSet.getTimestamp(2), equalTo(ts));
      }
    }
  }

  /**
   * Test that stage binding and regular binding insert and return the same value for timestamp_ntz
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testTimestampBindingWithNTZType() throws SQLException {
    TimeZone.setDefault(tokyoTz);
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table stageinsert(ind int, ltz0 timestamp_ltz, tz0 timestamp_tz, ntz0 timestamp_ntz)");
        statement.execute(
            "create or replace table regularinsert(ind int, ltz0 timestamp_ltz, tz0 timestamp_tz, ntz0 timestamp_ntz)");
        statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_NTZ");
        statement.execute("alter session set TIMEZONE='Asia/Tokyo'");
        Timestamp currT = new Timestamp(System.currentTimeMillis());

        // insert using regular binging
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into regularinsert values (?,?,?,?)")) {
          prepStatement.setInt(1, 1);
          prepStatement.setTimestamp(2, currT, tokyo);
          prepStatement.setTimestamp(3, currT, tokyo);
          prepStatement.setTimestamp(4, currT);
          prepStatement.addBatch();
          prepStatement.executeBatch();
        }
        // insert using stage binding
        statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
        executePsStatementForTimestampTest(connection, "stageinsert", currT);

        // Compare the results
        try (ResultSet rs1 = statement.executeQuery("select * from stageinsert");
            ResultSet rs2 = statement.executeQuery("select * from regularinsert")) {
          assertTrue(rs1.next());
          assertTrue(rs2.next());

          assertEquals(rs1.getInt(1), rs2.getInt(1));

          // Check tz type and ltz type columns have the same value.
          assertEquals(rs1.getTimestamp(2), rs1.getTimestamp(3));

          assertEquals(rs1.getTimestamp(2), rs2.getTimestamp(2));
          assertEquals(rs1.getTimestamp(3), rs2.getTimestamp(3));
          assertEquals(rs1.getTimestamp(4), rs2.getTimestamp(4));
        }
      } finally {
        statement.execute("drop table if exists stageinsert");
        statement.execute("drop table if exists regularinsert");
        TimeZone.setDefault(origTz);
      }
    }
  }

  /**
   * Test that stage binding and regular binding insert and return the same value for timestamp_ltz
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testTimestampBindingWithLTZType() throws SQLException {
    TimeZone.setDefault(tokyoTz);
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table stageinsert(ind int, ltz0 timestamp_ltz, tz0 timestamp_tz, ntz0 timestamp_ntz)");
        statement.execute(
            "create or replace table regularinsert(ind int, ltz0 timestamp_ltz, tz0 timestamp_tz, ntz0 timestamp_ntz)");
        statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ");
        statement.execute("alter session set TIMEZONE='Asia/Tokyo'");
        Timestamp currT = new Timestamp(System.currentTimeMillis());

        // insert using regular binging
        executePsStatementForTimestampTest(connection, "regularinsert", currT);

        // insert using stage binding
        statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
        executePsStatementForTimestampTest(connection, "stageinsert", currT);

        // Compare the results
        try (ResultSet rs1 = statement.executeQuery("select * from stageinsert");
            ResultSet rs2 = statement.executeQuery("select * from regularinsert")) {
          assertTrue(rs1.next());
          assertTrue(rs2.next());

          assertEquals(rs1.getInt(1), rs2.getInt(1));

          // Check that all the values are the same.
          assertEquals(rs1.getTimestamp(2), rs1.getTimestamp(3));
          assertEquals(rs1.getTimestamp(3), rs1.getTimestamp(4));

          assertEquals(rs1.getTimestamp(2), rs2.getTimestamp(2));
          assertEquals(rs1.getTimestamp(3), rs2.getTimestamp(3));
          assertEquals(rs1.getTimestamp(4), rs2.getTimestamp(4));
        }
      } finally {
        statement.execute("drop table if exists stageinsert");
        statement.execute("drop table if exists regularinsert");
        TimeZone.setDefault(origTz);
      }
    }
  }

  /**
   * Test that stage binding and regular binding insert and return the same value for timestamp_ltz
   * when the local timezone has the daylight saving. This test is added in version > 3.16.1
   *
   * <p>When CLIENT_TIMESTAMP_TYPE_MAPPING setting is mismatched with target data type (e.g
   * MAPPING=LTZ and insert to NTZ or MAPPING=NTZ and insert to TZ/LTZ there could be different
   * result as the timezone offset is applied on client side and removed on server side. This only
   * occurs around the boundary of daylight-savings and the difference from the source data would be
   * one hour. Both regular binding and stage binding have such issue but they also behave
   * diffently, for some data only regular binding gets the extra hour while sometime only stage
   * binding does. The workaround is to use CLIENT_TIMESTAMP_TYPE_MAPPING=LTZ to insert LTZ/TZ data
   * and use CLIENT_TIMESTAMP_TYPE_MAPPING=NTZ to insert NTZ data.
   *
   * <p>This test cannot run on the GitHub testing because of the "ALTER SESSION SET
   * CLIENT_STAGE_ARRAY_BINDING_THRESHOLD" This command should be executed with the system admin.
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testTimestampBindingWithLTZTypeForDayLightSavingTimeZone() throws SQLException {
    Calendar australia = Calendar.getInstance(australiaTz);
    TimeZone.setDefault(australiaTz);
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table stageinsert(ind int, ltz0 timestamp_ltz, ltz1 timestamp_ltz, ltz2 timestamp_ltz, tz0 timestamp_tz, tz1 timestamp_tz, tz2 timestamp_tz, ntz0 timestamp_ntz, ntz1 timestamp_ntz, ntz2 timestamp_ntz)");
        statement.execute(
            "create or replace table regularinsert(ind int, ltz0 timestamp_ltz, ltz1 timestamp_ltz, ltz2 timestamp_ltz, tz0 timestamp_tz, tz1 timestamp_tz, tz2 timestamp_tz, ntz0 timestamp_ntz, ntz1 timestamp_ntz, ntz2 timestamp_ntz)");
        statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ");
        statement.execute("alter session set TIMEZONE='UTC'");

        Timestamp ts1 = new Timestamp(1403049600000L);
        Timestamp ts2 = new Timestamp(1388016000000L);
        Timestamp ts3 = new Timestamp(System.currentTimeMillis());

        // insert using regular binging
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into regularinsert values (?,?,?,?,?,?,?,?,?,?)")) {
          prepStatement.setInt(1, 1);
          prepStatement.setTimestamp(2, ts1);
          prepStatement.setTimestamp(3, ts2);
          prepStatement.setTimestamp(4, ts3);

          prepStatement.setTimestamp(5, ts1);
          prepStatement.setTimestamp(6, ts2);
          prepStatement.setTimestamp(7, ts3);

          prepStatement.setTimestamp(8, ts1, australia);
          prepStatement.setTimestamp(9, ts2, australia);
          prepStatement.setTimestamp(10, ts3, australia);

          prepStatement.addBatch();
          prepStatement.executeBatch();
        }

        // insert using stage binding
        statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into stageinsert values (?,?,?,?,?,?,?,?,?,?)")) {
          prepStatement.setInt(1, 1);
          prepStatement.setTimestamp(2, ts1);
          prepStatement.setTimestamp(3, ts2);
          prepStatement.setTimestamp(4, ts3);

          prepStatement.setTimestamp(5, ts1);
          prepStatement.setTimestamp(6, ts2);
          prepStatement.setTimestamp(7, ts3);

          prepStatement.setTimestamp(8, ts1);
          prepStatement.setTimestamp(9, ts2);
          prepStatement.setTimestamp(10, ts3);

          prepStatement.addBatch();
          prepStatement.executeBatch();
        }

        // Compare the results
        try (ResultSet rs1 = statement.executeQuery("select * from stageinsert");
            ResultSet rs2 = statement.executeQuery("select * from regularinsert")) {
          assertTrue(rs1.next());
          assertTrue(rs2.next());

          assertEquals(rs1.getInt(1), rs2.getInt(1));
          assertEquals(rs1.getTimestamp(2), rs2.getTimestamp(2));
          assertEquals(rs1.getTimestamp(3), rs2.getTimestamp(3));
          assertEquals(rs1.getTimestamp(4), rs2.getTimestamp(4));
          assertEquals(rs1.getTimestamp(5), rs2.getTimestamp(5));
          assertEquals(rs1.getTimestamp(6), rs2.getTimestamp(6));
          assertEquals(rs1.getTimestamp(7), rs2.getTimestamp(7));
          assertEquals(rs1.getTimestamp(8), rs2.getTimestamp(8));
          assertEquals(rs1.getTimestamp(9), rs2.getTimestamp(9));
          assertEquals(rs1.getTimestamp(10), rs2.getTimestamp(10));

          assertEquals(ts1.getTime(), rs1.getTimestamp(2).getTime());
          assertEquals(ts2.getTime(), rs1.getTimestamp(3).getTime());
          assertEquals(ts3.getTime(), rs1.getTimestamp(4).getTime());
          assertEquals(ts1.getTime(), rs1.getTimestamp(5).getTime());
          assertEquals(ts2.getTime(), rs1.getTimestamp(6).getTime());
          assertEquals(ts3.getTime(), rs1.getTimestamp(7).getTime());
          assertEquals(ts1.getTime(), rs1.getTimestamp(8).getTime());
          assertEquals(ts2.getTime(), rs1.getTimestamp(9).getTime());
          assertEquals(ts3.getTime(), rs1.getTimestamp(10).getTime());

          assertEquals(ts1.getTime(), rs2.getTimestamp(2).getTime());
          assertEquals(ts2.getTime(), rs2.getTimestamp(3).getTime());
          assertEquals(ts3.getTime(), rs2.getTimestamp(4).getTime());
          assertEquals(ts1.getTime(), rs2.getTimestamp(5).getTime());
          assertEquals(ts2.getTime(), rs2.getTimestamp(6).getTime());
          assertEquals(ts3.getTime(), rs2.getTimestamp(7).getTime());
          assertEquals(ts1.getTime(), rs2.getTimestamp(8).getTime());
          assertEquals(ts2.getTime(), rs2.getTimestamp(9).getTime());
          assertEquals(ts3.getTime(), rs2.getTimestamp(10).getTime());
        }
      } finally {
        statement.execute("drop table if exists stageinsert");
        statement.execute("drop table if exists regularinsert");
        TimeZone.setDefault(origTz);
      }
    }
  }

  public void executePsStatementForTimestampTest(
      Connection connection, String tableName, Timestamp timestamp) throws SQLException {
    try (PreparedStatement prepStatement =
        connection.prepareStatement("insert into " + tableName + " values (?,?,?,?)")) {
      prepStatement.setInt(1, 1);
      prepStatement.setTimestamp(2, timestamp);
      prepStatement.setTimestamp(3, timestamp);
      prepStatement.setTimestamp(4, timestamp);
      prepStatement.addBatch();
      prepStatement.executeBatch();
    }
  }
}
