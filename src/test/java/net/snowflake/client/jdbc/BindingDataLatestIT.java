/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Binding Data integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the version of oldest supported driver
 * to examine if the tests still are not applicable. If it is applicable, move tests to
 * BindingDataIT so that both the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class BindingDataLatestIT extends AbstractDriverIT {
  @Test
  public void testBindTimestampTZ() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace table testBindTimestampTZ(" + "cola int, colb timestamp_tz)");
      statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_TZ");

      long millSeconds = System.currentTimeMillis();
      Timestamp ts = new Timestamp(millSeconds);
      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into testBindTimestampTZ values (?, ?)")) {
        prepStatement.setInt(1, 123);
        prepStatement.setTimestamp(2, ts, Calendar.getInstance(TimeZone.getTimeZone("EST")));
        prepStatement.execute();
      }

      try (ResultSet resultSet =
          statement.executeQuery("select cola, colb from testBindTimestampTz")) {
        resultSet.next();
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testTimestampBindingWithNTZType() throws SQLException {
    TimeZone origTz = TimeZone.getDefault();
    Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));

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
          for (int i = 1; i <= 6; i++) {
            prepStatement.setInt(1, 1);
            prepStatement.setTimestamp(2, currT);
            prepStatement.setTimestamp(3, currT);
            prepStatement.setTimestamp(4, currT);
            prepStatement.addBatch();
          }
          prepStatement.executeBatch();
        }

        // insert using stage binding
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into stageinsert values (?,?,?,?)")) {
          statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
          prepStatement.setInt(1, 1);
          prepStatement.setTimestamp(2, currT, utc);
          prepStatement.setTimestamp(3, currT, utc);
          prepStatement.setTimestamp(4, currT);
          prepStatement.addBatch();
          prepStatement.executeBatch();
        }

        // Compare the results
        try (ResultSet rs1 = statement.executeQuery("select * from stageinsert");
            ResultSet rs2 = statement.executeQuery("select * from regularinsert")) {
          rs1.next();
          rs2.next();

          assertEquals(rs1.getInt(1), rs2.getInt(1));

          // Check all the columns have the same.
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
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testTimestampBindingWithLTZType() throws SQLException {
    TimeZone origTz = TimeZone.getDefault();
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create or replace table stageinsert(ind int, ltz0 timestamp_ltz, tz0 timestamp_tz, ntz0 timestamp_ntz)");
        statement.execute(
            "create or replace table regularinsert(ind int, ltz0 timestamp_ltz, tz0 timestamp_tz, ntz0 timestamp_ntz)");
        statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ");
        statement.execute("alter session set TIMEZONE='Asia/Tokyo'");
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
        Timestamp currT = new Timestamp(System.currentTimeMillis());

        // insert using regular binging
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into regularinsert values (?,?,?,?)")) {
          for (int i = 1; i <= 6; i++) {
            prepStatement.setInt(1, 1);
            prepStatement.setTimestamp(2, currT);
            prepStatement.setTimestamp(3, currT);
            prepStatement.setTimestamp(4, currT);
            prepStatement.addBatch();
          }
          prepStatement.executeBatch();
        }

        // insert using stage binding
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into stageinsert values (?,?,?,?)")) {
          statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
          prepStatement.setInt(1, 1);
          prepStatement.setTimestamp(2, currT);
          prepStatement.setTimestamp(3, currT);
          prepStatement.setTimestamp(4, currT);
          prepStatement.addBatch();
          prepStatement.executeBatch();
        }

        // Compare the results
        try (ResultSet rs1 = statement.executeQuery("select * from stageinsert");
            ResultSet rs2 = statement.executeQuery("select * from regularinsert")) {
          rs1.next();
          rs2.next();

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
   *
   * @throws SQLException
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testTimestampBindingWithLTZTypeForDayLightSavingTimeZone() throws SQLException {
    TimeZone origTz = TimeZone.getDefault();
    Calendar australia = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"));
    TimeZone.setDefault(TimeZone.getTimeZone("Australia/Sydney"));
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
          for (int i = 1; i <= 6; i++) {
            prepStatement.setInt(i, 1);
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
          }
          prepStatement.executeBatch();
        }

        // insert using stage binding
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into stageinsert values (?,?,?,?,?,?,?,?,?,?)")) {
          statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
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
          rs1.next();
          rs2.next();

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
}
