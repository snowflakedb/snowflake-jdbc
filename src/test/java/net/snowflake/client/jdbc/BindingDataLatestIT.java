package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
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

  /**
   * Test that binding TIMESTAMP_LTZ does not affect other date time bindings time zones as a side
   * effect
   *
   * <p>This test cannot run on the GitHub testing because of the "ALTER SESSION SET
   * CLIENT_STAGE_ARRAY_BINDING_THRESHOLD" This command should be executed with the system admin.
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testTimestampLtzBindingNoLongerBreaksOtherDatetimeBindings() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table stageinsertdates(ind int, t1 timestamp_ltz, d1 date)");
        Date date1 =
            new java.sql.Date(
                ZonedDateTime.of(2016, 2, 19, 0, 0, 0, 0, ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli());
        Timestamp ts1 = new Timestamp(date1.getTime());
        // insert using stage binding
        statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into stageinsertdates values (?,?, ?)")) {
          prepStatement.setInt(1, 1);
          prepStatement.setTimestamp(2, ts1);
          prepStatement.setDate(3, date1);
          prepStatement.addBatch();
          prepStatement.executeBatch();
          prepStatement.getConnection().commit();
        }

        try (ResultSet rs1 = statement.executeQuery("select * from stageinsertdates")) {
          assertTrue(rs1.next());

          assertEquals(1, rs1.getInt(1));
          assertEquals(ts1, rs1.getTimestamp(2));
          assertEquals(date1, rs1.getDate(3));
        }
      } finally {
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
