package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Test ResultSet */
@Tag(TestTags.RESULT_SET)
public class ResultSetMultiTimeZoneIT extends BaseJDBCTest {
  static TimeZone ogTz;

  static class DataProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      List<String> timezones =
          new ArrayList<String>() {
            {
              add("UTC");
              add("Asia/Singapore");
              add("CET");
            }
          };
      List<String> queryFormats =
          new ArrayList<String>() {
            {
              add("json");
              add("arrow");
            }
          };

      List<Arguments> args = new ArrayList<>();
      for (String timeZone : timezones) {
        for (String queryFormat : queryFormats) {
          args.add(Arguments.argumentSet(timeZone + " " + queryFormat, timeZone, queryFormat));
        }
      }

      return args.stream();
    }
  }

  @BeforeAll
  public static void setDefaultTimezone() {
    ogTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  private static void setTimezone(String timeZone) {
    System.setProperty("user.timezone", timeZone);
  }

  @AfterAll
  public static void clearTimezone() {
    TimeZone.setDefault(ogTz);
    System.clearProperty("user.timezone");
  }

  public Connection init(String queryResultFormat) throws SQLException {
    Connection connection = BaseJDBCTest.getConnection();

    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "alter session set "
              + "TIMEZONE='America/Los_Angeles',"
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
      statement.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return connection;
  }

  public Connection init() throws SQLException {
    Connection connection = BaseJDBCTest.getConnection();

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
    return connection;
  }

  @BeforeEach
  public void setUp() throws SQLException {
    try (Connection con = init();
        Statement statement = con.createStatement()) {

      // TEST_RS
      statement.execute("create or replace table test_rs (colA string)");
      statement.execute("insert into test_rs values('rowOne')");
      statement.execute("insert into test_rs values('rowTwo')");
      statement.execute("insert into test_rs values('rowThree')");

      // ORDERS_JDBC
      statement.execute(
          "create or replace table orders_jdbc"
              + "(C1 STRING NOT NULL COMMENT 'JDBC', "
              + "C2 STRING, C3 STRING, C4 STRING, C5 STRING, C6 STRING, "
              + "C7 STRING, C8 STRING, C9 STRING) "
              + "stage_file_format = (field_delimiter='|' "
              + "error_on_column_count_mismatch=false)");
      // put files
      assertTrue(
          statement.execute(
              "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @%orders_jdbc"),
          "Failed to put a file");
      assertTrue(
          statement.execute(
              "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE_2) + " @%orders_jdbc"),
          "Failed to put a file");

      int numRows = statement.executeUpdate("copy into orders_jdbc");

      assertEquals(73, numRows, "Unexpected number of rows copied: " + numRows);
    }
  }

  @AfterEach
  public void tearDown() throws SQLException {
    System.clearProperty("user.timezone");
    try (Connection con = init();
        Statement statement = con.createStatement()) {
      statement.execute("drop table if exists orders_jdbc");
      statement.execute("drop table if exists test_rs");
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testGetDateAndTime(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table dateTime(colA Date, colB Timestamp, colC Time)");

        java.util.Date today = new java.util.Date();
        Date date = buildDate(2016, 3, 20);
        Timestamp ts = new Timestamp(today.getTime());
        Time tm = new Time(12345678); // 03:25:45.678
        final String insertTime = "insert into datetime values(?, ?, ?)";
        try (PreparedStatement prepStatement = connection.prepareStatement(insertTime)) {
          prepStatement.setDate(1, date);
          prepStatement.setTimestamp(2, ts);
          prepStatement.setTime(3, tm);

          prepStatement.execute();

          ResultSet resultSet = statement.executeQuery("select * from datetime");
          assertTrue(resultSet.next());
          assertEquals(date, resultSet.getDate(1));
          assertEquals(date, resultSet.getDate("COLA"));
          assertEquals(ts, resultSet.getTimestamp(2));
          assertEquals(ts, resultSet.getTimestamp("COLB"));
          assertEquals(tm, resultSet.getTime(3));
          assertEquals(tm, resultSet.getTime("COLC"));
        }
        statement.execute(
            "create or replace table datetime(colA timestamp_ltz, colB timestamp_ntz, colC timestamp_tz)");
        statement.execute(
            "insert into dateTime values ('2019-01-01 17:17:17', '2019-01-01 17:17:17', '2019-01-01 "
                + "17:17:17')");
        try (PreparedStatement prepStatement =
            connection.prepareStatement(
                "insert into datetime values(?, '2019-01-01 17:17:17', '2019-01-01 17:17:17')")) {
          Timestamp dateTime = new Timestamp(date.getTime());
          prepStatement.setTimestamp(1, dateTime);
          prepStatement.execute();
          try (ResultSet resultSet = statement.executeQuery("select * from datetime")) {
            assertTrue(resultSet.next());
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            formatter.setTimeZone(TimeZone.getDefault());
            String d = formatter.format(resultSet.getDate("COLA"));
            assertEquals("2019-01-02 01:17:17", d);
            assertTrue(resultSet.next());
            assertEquals(date, resultSet.getDate(1));
            assertEquals(date, resultSet.getDate("COLA"));
          }
        }
      } finally {
        statement.execute("drop table if exists datetime");
      }
    }
  }

  // SNOW-25029: The driver should reduce Time milliseconds mod 24h.
  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testTimeRange(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    final String insertTime = "insert into timeTest values (?), (?), (?), (?)";
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table timeTest (c1 time)");

        long ms1 = -2202968667333L; // 1900-03-11 09:15:33.667
        long ms2 = -1; // 1969-12-31 23:59:99.999
        long ms3 = 86400 * 1000; // 1970-01-02 00:00:00
        long ms4 = 1451680250123L; // 2016-01-01 12:30:50.123

        Time tm1 = new Time(ms1);
        Time tm2 = new Time(ms2);
        Time tm3 = new Time(ms3);
        Time tm4 = new Time(ms4);

        try (PreparedStatement prepStatement = connection.prepareStatement(insertTime)) {
          prepStatement.setTime(1, tm1);
          prepStatement.setTime(2, tm2);
          prepStatement.setTime(3, tm3);
          prepStatement.setTime(4, tm4);

          prepStatement.execute();
        }

        // Note that the resulting Time objects are NOT equal because they have
        // their milliseconds in the range 0 to 86,399,999, i.e. inside Jan 1, 1970.
        // PreparedStatement accepts Time objects outside this range, but it reduces
        // modulo 24 hours to discard the date information before sending to GS.

        final long M = 86400 * 1000;
        try (ResultSet resultSet = statement.executeQuery("select * from timeTest")) {
          assertTrue(resultSet.next());
          assertNotEquals(tm1, resultSet.getTime(1));
          assertEquals(new Time((ms1 % M + M) % M), resultSet.getTime(1));
          assertTrue(resultSet.next());
          assertNotEquals(tm2, resultSet.getTime(1));
          assertEquals(new Time((ms2 % M + M) % M), resultSet.getTime(1));
          assertTrue(resultSet.next());
          assertNotEquals(tm3, resultSet.getTime(1));
          assertEquals(new Time((ms3 % M + M) % M), resultSet.getTime(1));
          assertTrue(resultSet.next());
          assertNotEquals(tm4, resultSet.getTime(1));
          assertEquals(new Time((ms4 % M + M) % M), resultSet.getTime(1));
        }
      } finally {
        statement.execute("drop table if exists timeTest");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testCurrentTime(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    final String insertTime = "insert into datetime values (?, ?, ?)";
    try (Connection connection = init(queryResultFormat)) {

      assertFalse(connection.createStatement().execute("alter session set TIMEZONE='UTC'"));

      try (Statement statement = connection.createStatement()) {
        try {
          statement.execute("create or replace table datetime (d date, ts timestamp, tm time)");
          try (PreparedStatement prepStatement = connection.prepareStatement(insertTime)) {

            long currentMillis = System.currentTimeMillis();
            Date currentDate = new Date(currentMillis);
            Timestamp currentTS = new Timestamp(currentMillis);
            Time currentTime = new Time(currentMillis);

            prepStatement.setDate(1, currentDate);
            prepStatement.setTimestamp(2, currentTS);
            prepStatement.setTime(3, currentTime);

            prepStatement.execute();

            try (ResultSet resultSet =
                statement.executeQuery("select ts::date = d from datetime")) {
              assertTrue(resultSet.next());
              assertTrue(resultSet.getBoolean(1));
            }
            try (ResultSet resultSet =
                statement.executeQuery("select ts::time = tm from datetime")) {
              assertTrue(resultSet.next());
              assertTrue(resultSet.getBoolean(1));
            }
          }
        } finally {
          statement.execute("drop table if exists datetime");
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testBindTimestampTZ(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table testBindTimestampTZ(" + "cola int, colb timestamp_tz)");

        long millSeconds = System.currentTimeMillis();
        Timestamp ts = new Timestamp(millSeconds);
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into testBindTimestampTZ values (?, ?)")) {
          prepStatement.setInt(1, 123);
          prepStatement.setTimestamp(2, ts, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
          prepStatement.execute();
        }

        try (ResultSet resultSet =
            statement.executeQuery("select cola, colb from testBindTimestampTz")) {
          assertTrue(resultSet.next());
          assertThat("integer", resultSet.getInt(1), equalTo(123));
          assertThat("timestamp_tz", resultSet.getTimestamp(2), equalTo(ts));
        }
      } finally {
        statement.execute("drop table if exists testBindTimestampTZ");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testGetOldDate(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table testOldDate(d date)");
        statement.execute(
            "insert into testOldDate values ('0001-01-01'), "
                + "(to_date('1000-01-01')), ('1300-01-01'), ('1400-02-02'), "
                + "('1500-01-01'), ('1600-02-03')");

        try (ResultSet resultSet = statement.executeQuery("select * from testOldDate order by d")) {
          assertTrue(resultSet.next());
          assertEquals("0001-01-01", resultSet.getString(1));
          assertEquals(Date.valueOf("0001-01-01"), resultSet.getDate(1));
          assertTrue(resultSet.next());
          assertEquals("1000-01-01", resultSet.getString(1));
          assertEquals(Date.valueOf("1000-01-01"), resultSet.getDate(1));
          assertTrue(resultSet.next());
          assertEquals("1300-01-01", resultSet.getString(1));
          assertEquals(Date.valueOf("1300-01-01"), resultSet.getDate(1));
          assertTrue(resultSet.next());
          assertEquals("1400-02-02", resultSet.getString(1));
          assertEquals(Date.valueOf("1400-02-02"), resultSet.getDate(1));
          assertTrue(resultSet.next());
          assertEquals("1500-01-01", resultSet.getString(1));
          assertEquals(Date.valueOf("1500-01-01"), resultSet.getDate(1));
          assertTrue(resultSet.next());
          assertEquals("1600-02-03", resultSet.getString(1));
          assertEquals(Date.valueOf("1600-02-03"), resultSet.getDate(1));
        }
      } finally {
        statement.execute("drop table if exists testOldDate");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testGetStringForDates(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      String expectedDate1 = "2020-08-01";
      String expectedDate2 = "1920-11-11";
      try (ResultSet rs = statement.executeQuery("SELECT '" + expectedDate1 + "'::DATE as D1")) {
        rs.next();
        assertEquals(expectedDate1, rs.getString(1));
      }
      try (ResultSet rs = statement.executeQuery("SELECT '" + expectedDate2 + "'::DATE as D1")) {
        rs.next();
        assertEquals(expectedDate2, rs.getString(1));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testDateTimeRelatedTypeConversion(String tz, String queryResultFormat)
      throws SQLException {
    setTimezone(tz);
    try (Connection connection = init(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table testDateTime"
                + "(colDate DATE, colTS timestamp_ltz, colTime TIME, colString string)");
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into testDateTime values(?, ?, ?, ?)")) {
          Timestamp ts = buildTimestamp(2016, 3, 20, 3, 25, 45, 67800000);
          Date date = buildDate(2016, 3, 20);
          Time time = new Time(12345678); // 03:25:45.678

          preparedStatement.setDate(1, date);
          preparedStatement.setTimestamp(2, ts);
          preparedStatement.setTime(3, time);
          preparedStatement.setString(4, "aaa");

          preparedStatement.execute();
          try (ResultSet resultSet = statement.executeQuery("select * from testDateTime")) {
            assertTrue(resultSet.next());

            // ResultSet.getDate()
            assertEquals(date, resultSet.getDate("COLDATE"));

            SnowflakeSQLException e =
                assertThrows(SnowflakeSQLException.class, () -> resultSet.getDate("COLTIME"));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), e.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), e.getSQLState());

            // ResultSet.getTimestamp()
            assertEquals(new Timestamp(date.getTime()), resultSet.getTimestamp("COLDATE"));
            assertEquals(ts, resultSet.getTimestamp("COLTS"));
            assertEquals(new Timestamp(time.getTime()), resultSet.getTimestamp("COLTIME"));

            e =
                assertThrows(
                    SnowflakeSQLException.class, () -> resultSet.getTimestamp("COLSTRING"));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), e.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), e.getSQLState());

            // ResultSet.getTime()
            e = assertThrows(SnowflakeSQLException.class, () -> resultSet.getTime("COLDATE"));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), e.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), e.getSQLState());

            assertEquals(time, resultSet.getTime("COLTIME"));
            assertEquals(new Time(ts.getTime()), resultSet.getTime("COLTS"));
          }
        }
      } finally {
        statement.execute("drop table if exists testDateTime");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testGetOldTimestamp(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table testOldTs(cola timestamp_ntz)");
        statement.execute(
            "insert into testOldTs values ('1582-06-22 17:00:00'), " + "('1000-01-01 17:00:00')");

        try (ResultSet resultSet = statement.executeQuery("select * from testOldTs")) {

          assertTrue(resultSet.next());

          assertThat(resultSet.getTimestamp(1).toString(), equalTo("1582-06-22 17:00:00.0"));
          assertThat(resultSet.getString(1), equalTo("Fri, 22 Jun 1582 17:00:00 Z"));

          assertTrue(resultSet.next());
          assertThat(resultSet.getTimestamp(1).toString(), equalTo("1000-01-01 17:00:00.0"));
          assertThat(resultSet.getString(1), equalTo("Mon, 01 Jan 1000 17:00:00 Z"));
        }
      } finally {
        statement.execute("drop table if exists testOldTs");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnGithubActions
  public void testPrepareOldTimestamp(String tz, String queryResultFormat) throws SQLException {
    setTimezone(tz);
    TimeZone origTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table testPrepOldTs(cola timestamp_ntz, colb date)");
        statement.execute("alter session set client_timestamp_type_mapping=timestamp_ntz");
        PreparedStatement ps = con.prepareStatement("insert into testPrepOldTs values (?, ?)");

        ps.setTimestamp(1, Timestamp.valueOf("0001-01-01 08:00:00"));
        ps.setDate(2, Date.valueOf("0001-01-01"));
        ps.executeUpdate();

        ResultSet resultSet = statement.executeQuery("select * from testPrepOldTs");

        assertTrue(resultSet.next());
        assertThat(resultSet.getTimestamp(1).toString(), equalTo("0001-01-01 08:00:00.0"));
        assertThat(resultSet.getDate(2).toString(), equalTo("0001-01-01"));
      } finally {
        statement.execute("drop table if exists testPrepOldTs");
        TimeZone.setDefault(origTz);
      }
    }
  }
}
