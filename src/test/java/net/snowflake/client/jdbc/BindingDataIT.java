package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Integration tests for binding variable */
@Tag(TestTags.OTHERS)
public class BindingDataIT extends BaseJDBCWithSharedConnectionIT {
  static TimeZone timeZone;

  @BeforeAll
  public static void setTimeZone() {
    timeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterAll
  public static void resetTimeZone() {
    TimeZone.setDefault(timeZone);
  }

  @ParameterizedTest
  @ValueSource(shorts = {0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE})
  public void testBindShort(short shortValue) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_short(c1 number)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_short values (?)")) {
          preparedStatement.setShort(1, shortValue);
          assertEquals(1, preparedStatement.executeUpdate());
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_short where c1 = ?")) {
          preparedStatement.setShort(1, shortValue);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getShort("C1"), is(shortValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_short");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(shorts = {0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE})
  public void testBindShortViaSetObject(short shortValue) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_short(c1 number)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_short values (?)")) {
          preparedStatement.setObject(1, new Short(shortValue));
          preparedStatement.executeUpdate();
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_short where c1 = ?")) {
          preparedStatement.setObject(1, new Short(shortValue));

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getShort("C1"), is(shortValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_short");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE})
  public void testBindInt(int intValue) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_int(c1 number)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_int values (?)")) {
          preparedStatement.setInt(1, intValue);
          preparedStatement.executeUpdate();
        }

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_int where c1 = ?")) {
          preparedStatement.setInt(1, intValue);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt("C1"), is(intValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_int");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(bytes = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE})
  public void testBindByte(byte byteValue) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_byte(c1 integer)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_byte values (?)")) {
          preparedStatement.setByte(1, byteValue);
          preparedStatement.executeUpdate();
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_byte where c1 = ?")) {
          preparedStatement.setInt(1, byteValue);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getByte("C1"), is(byteValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_byte");
      }
    }
  }

  @Test
  public void testBindNull() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_null(id number, val " + "number)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_null values (?, ?)")) {
          preparedStatement.setInt(1, 0);
          preparedStatement.setBigDecimal(2, null);
          preparedStatement.addBatch();

          preparedStatement.setInt(1, 1);
          preparedStatement.setNull(1, Types.INTEGER);
          preparedStatement.addBatch();

          preparedStatement.setInt(1, 2);
          preparedStatement.setObject(1, null, Types.BIGINT);
          preparedStatement.addBatch();

          preparedStatement.setInt(1, 3);
          preparedStatement.setObject(1, null, Types.BIGINT, 0);
          preparedStatement.addBatch();

          preparedStatement.executeBatch();

          try (ResultSet rs =
              statement.executeQuery("select * from test_bind_null " + "order by id asc")) {
            int count = 0;
            while (rs.next()) {
              assertThat(rs.getBigDecimal("VAL"), is(nullValue()));
              count++;
            }

            assertThat(count, is(4));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_null");
      }
    }
  }

  static class TimeProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of(Time.valueOf("00:00:00")),
          Arguments.of(Time.valueOf("12:34:56")),
          Arguments.of(Time.valueOf("12:00:00")),
          Arguments.of(Time.valueOf("11:59:59")),
          Arguments.of(Time.valueOf("15:30:00")),
          Arguments.of(Time.valueOf("13:01:01")));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TimeProvider.class)
  public void testBindTime(Time timeVal) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_time(c1 time)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_time values (?)")) {
          preparedStatement.setTime(1, timeVal);
          preparedStatement.executeUpdate();
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_time where c1 = ?")) {
          preparedStatement.setTime(1, timeVal);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTime("C1"), is(timeVal));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_time");
      }
    }
  }

  /**
   * Bind time with calendar is not supported now. Everything is in UTC, need to revisit in the
   * future
   */
  @ParameterizedTest
  @ArgumentsSource(TimeProvider.class)
  public void testBindTimeWithCalendar(Time timeVal) throws SQLException {
    Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Calendar laCal = Calendar.getInstance(TimeZone.getTimeZone("PST"));

    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_time_calendar(c1 " + "time)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_time_calendar values (?)")) {
          preparedStatement.setTime(1, timeVal, laCal);
          preparedStatement.executeUpdate();
        }

        // bind time with UTC
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_time_calendar where c1 = ?")) {
          preparedStatement.setTime(1, timeVal, laCal);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTime("C1", utcCal), is(timeVal));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_time_calendar");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TimeProvider.class)
  public void testBindTimeViaSetObject(Time timeVal) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_time(c1 time)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_time values (?)")) {
          preparedStatement.setObject(1, timeVal, Types.TIME);
          preparedStatement.executeUpdate();
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_time where c1 = ?")) {
          preparedStatement.setObject(1, timeVal, Types.TIME);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTime("C1"), is(timeVal));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_time");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TimeProvider.class)
  public void testBindTimeViaSetObjectCast(Time timeVal) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_time(c1 time)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_time values (?)")) {
          preparedStatement.setObject(1, timeVal);
          preparedStatement.executeUpdate();
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_time where c1 = ?")) {
          preparedStatement.setObject(1, timeVal);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getTime("C1"), is(timeVal));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_time");
      }
    }
  }

  static class DateProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of(Date.valueOf("2000-01-01")),
          Arguments.of(Date.valueOf("3000-01-01")),
          Arguments.of(Date.valueOf("1970-01-01")),
          Arguments.of(Date.valueOf("1969-01-01")),
          Arguments.of(Date.valueOf("1500-01-01")),
          Arguments.of(Date.valueOf("1400-01-01")),
          Arguments.of(Date.valueOf("1000-01-01")));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DateProvider.class)
  public void testBindDate(Date dateValue) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_date(c1 date)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_date values (?)")) {
          preparedStatement.setDate(1, dateValue);
          preparedStatement.executeUpdate();
        }

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_date where c1 = ?")) {
          preparedStatement.setDate(1, dateValue);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getDate("C1"), is(dateValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_date");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DateProvider.class)
  public void testBindDateWithCalendar(Date dateValue) throws SQLException {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_date(c1 date)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_date values (?)")) {
          preparedStatement.setDate(1, dateValue, calendar);
          preparedStatement.executeUpdate();
        }

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_date where c1 = ?")) {
          preparedStatement.setDate(1, dateValue, calendar);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getDate("C1"), is(dateValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_date");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE})
  public void testBindObjectWithScaleZero(int intValue) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_bind_object_0(c1 number)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_bind_object_0 values (?)")) {
          preparedStatement.setObject(1, intValue, Types.NUMERIC, 0);
          preparedStatement.executeUpdate();
        }

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_bind_object_0 where c1 = ?")) {
          preparedStatement.setObject(1, intValue, Types.NUMERIC, 0);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt("C1"), is(intValue));
          }
        }
      } finally {
        statement.execute("drop table if exists test_bind_object_0");
      }
    }
  }

  /** Binding null as all types. */
  @Test
  public void testBindNullForAllTypes() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace table TEST_BIND_ALL_TYPES(C0 string,"
              + "C1 number(20, 3), C2 INTEGER, C3 double, C4 varchar(1000),"
              + "C5 string, C6 date, C7 time, C8 timestamp_ntz, "
              + "C9 timestamp_ltz, C10 timestamp_tz,"
              + "C11 BINARY, C12 BOOLEAN)");

      for (SnowflakeType.JavaSQLType t : SnowflakeType.JavaSQLType.ALL_TYPES) {
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "insert into TEST_BIND_ALL_TYPES values(?, ?,?,?, ?,?,?, ?,?,?, ?,?,?)")) {
          preparedStatement.setString(1, t.toString());
          for (int i = 2; i <= 13; ++i) {
            preparedStatement.setNull(i, t.getType());
          }
          preparedStatement.executeUpdate();
        }
      }

      try (ResultSet result = statement.executeQuery("select * from TEST_BIND_ALL_TYPES")) {
        while (result.next()) {
          String testType = result.getString(1);
          for (int i = 2; i <= 13; ++i) {
            assertNull(result.getString(i), String.format("Java Type: %s is not null", testType));
          }
        }
      }
    }
  }
}
