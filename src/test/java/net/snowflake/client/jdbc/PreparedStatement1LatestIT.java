package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.PreparedStatement1IT.bindOneParamSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * PreparedStatement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still are not applicable. If it is applicable, move tests to PreparedStatement1IT so
 * that both the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.STATEMENT)
public class PreparedStatement1LatestIT extends PreparedStatement0IT {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepStWithCacheEnabled(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      // ensure enable the cache result use
      statement.execute(enableCacheReuse);

      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.execute();
        prepStatement.execute();
        bindOneParamSet(prepStatement, 100, 1.2222, (float) 1.2, "testA", 12122L, (short) 12);
        prepStatement.execute();
      }

      try (ResultSet resultSet = statement.executeQuery("select * from test_prepst")) {
        assertTrue(resultSet.next());
        assertEquals(resultSet.getInt(1), 1);
        assertTrue(resultSet.next());
        assertEquals(resultSet.getInt(1), 1);
        assertTrue(resultSet.next());
        assertEquals(resultSet.getInt(1), 100);
      }

      try (PreparedStatement prepStatement =
          connection.prepareStatement("select id, id + ? from test_prepst where id  = ?")) {
        prepStatement.setInt(1, 1);
        prepStatement.setInt(2, 1);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(resultSet.getInt(2), 2);
          prepStatement.setInt(1, 1);
          prepStatement.setInt(2, 100);
        }
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertEquals(resultSet.getInt(2), 101);
        }
      }
      try (PreparedStatement prepStatement =
          connection.prepareStatement(
              "select seq4() from table(generator(rowcount=>100)) limit ?")) {
        prepStatement.setInt(1, 1);

        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
          prepStatement.setInt(1, 3);
        }
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.next());
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
        }
      }
    }
  }
  /**
   * Test to ensure it's possible to upload Time values via stage array binding and get proper
   * values back (SNOW-194437)
   *
   * <p>Ignored on GitHub Action because CLIENT_STAGE_ARRAY_BINDING_THRESHOLD parameter is not
   * available to customers so cannot be set when running on Github Action
   *
   * @throws SQLException arises if any exception occurs
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testInsertStageArrayBindWithTime(String queryResultFormat) throws SQLException {
    TimeZone originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("alter session set CLIENT_STAGE_ARRAY_BINDING_THRESHOLD=2");
        statement.execute("create or replace table testStageBindTime (c1 time, c2 time)");
        PreparedStatement prepSt =
            connection.prepareStatement("insert into testStageBindTime values (?, ?)");
        Time[][] timeValues = {
          {new Time(0), new Time(1)},
          {new Time(1000), new Time(Integer.MAX_VALUE)},
          {new Time(123456), new Time(55555)},
          {Time.valueOf("01:02:00"), new Time(-100)},
        };
        for (Time[] value : timeValues) {
          prepSt.setTime(1, value[0]);
          prepSt.setTime(2, value[1]);
          prepSt.addBatch();
        }
        prepSt.executeBatch();
        // check results
        try (ResultSet rs = statement.executeQuery("select * from testStageBindTime")) {
          for (Time[] timeValue : timeValues) {
            assertTrue(rs.next());
            assertEquals(timeValue[0].toString(), rs.getTime(1).toString());
            assertEquals(timeValue[1].toString(), rs.getTime(2).toString());
          }
        }
      } finally {
        statement.execute("drop table if exists testStageBindTime");
        statement.execute("alter session unset CLIENT_STAGE_ARRAY_BINDING_THRESHOLD");
        TimeZone.setDefault(originalTimeZone);
      }
    }
  }

  /**
   * Test that setObject() with additional NTZ and LTZ timestamp types gives same results for stage
   * array bind and payload binding, regardless of CLIENT_TIMESTAMP_TYPE_MAPPING parameter value.
   * For SNOW-259255
   *
   * <p>Ignored on GitHub Action because CLIENT_STAGE_ARRAY_BINDING_THRESHOLD parameter is not
   * available to customers so cannot be set when running on Github Action
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testSetObjectForTimestampTypes(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      // set timestamp mapping to default value
      try {
        statement.execute("ALTER SESSION UNSET CLIENT_TIMESTAMP_TYPE_MAPPING");
        statement.execute("create or replace table TS (ntz TIMESTAMP_NTZ, ltz TIMESTAMP_LTZ)");
        PreparedStatement prepst = connection.prepareStatement("insert into TS values (?, ?)");
        String date1 = "2014-01-01 16:00:00";
        String date2 = "1945-11-12 5:25:00";
        Timestamp[] testTzs = {Timestamp.valueOf(date1), Timestamp.valueOf(date2)};
        for (int i = 0; i < testTzs.length; i++) {
          // Disable stage array binding and insert the timestamp values
          statement.execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0"); // disable stage bind
          prepst.setObject(1, testTzs[i], SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ);
          prepst.setObject(2, testTzs[i], SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ);
          prepst.addBatch();
          prepst.executeBatch();
          // Enable stage array binding and insert the same timestamp values as above
          statement.execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
          prepst.setObject(1, testTzs[i], SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ);
          prepst.setObject(2, testTzs[i], SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ);
          prepst.addBatch();
          prepst.executeBatch();
        }
        try (ResultSet rs = statement.executeQuery("select * from TS")) {
          // Get results for each timestamp value tested
          for (int i = 0; i < testTzs.length; i++) {
            // Assert that the first row of inserts with payload binding matches the second row of
            // inserts that used stage array binding
            assertTrue(rs.next());
            Timestamp expectedNTZTs = rs.getTimestamp(1);
            Timestamp expectedLTZTs = rs.getTimestamp(2);
            assertTrue(rs.next());
            assertEquals(expectedNTZTs, rs.getTimestamp(1));
            assertEquals(expectedLTZTs, rs.getTimestamp(2));
          }
        }
      } finally {
        statement.execute("ALTER SESSION UNSET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD;");
      }
    }
  }

  /**
   * Test to ensure when giving no input batch data, no exceptions will be thrown
   *
   * <p>Ignored on GitHub Action because CLIENT_STAGE_ARRAY_BINDING_THRESHOLD parameter is not
   * available to customers so cannot be set when running on Github Action
   *
   * @throws SQLException arises if any exception occurs
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testExecuteEmptyBatch(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        // executeBatch shouldn't throw exceptions
        assertEquals(
            0, prepStatement.executeBatch().length, "For empty batch, we should return int[0].");
      }

      connection
          .createStatement()
          .execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0"); // disable stage bind
      // we need a new PreparedStatement to pick up the changed status (not use stage bind)
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        // executeBatch shouldn't throw exceptions
        assertEquals(
            0, prepStatement.executeBatch().length, "For empty batch, we should return int[0].");
      }
    }
  }

  /**
   * Tests that VARBINARY columns can be set in setObject() method using byte[]
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSetObjectMethodWithVarbinaryColumn(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      connection.createStatement().execute("create or replace table test_binary(b VARBINARY)");

      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into test_binary(b) values (?)")) {
        prepStatement.setObject(1, "HELLO WORLD".getBytes());
        prepStatement.execute(); // shouldn't raise an error.
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSetObjectMethodWithBigIntegerColumn(String queryResultFormat) {
    try (Connection connection = getConn(queryResultFormat)) {
      connection.createStatement().execute("create or replace table test_bigint(id NUMBER)");

      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into test_bigint(id) values(?)")) {
        prepStatement.setObject(1, BigInteger.valueOf(9999));
        int rows = prepStatement.executeUpdate();
        assertTrue(rows == 1, "Row count doesn't match");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(
          "testSetObjectMethodWithBigIntegerColumn failed with an exception message: "
              + e.getMessage());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSetObjectMethodWithLargeBigIntegerColumn(String queryResultFormat) {
    try (Connection connection = getConn(queryResultFormat)) {
      connection.createStatement().execute("create or replace table test_bigint(id NUMBER)");

      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into test_bigint(id) values(?)")) {
        BigInteger largeBigInt = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN);
        prepStatement.setObject(1, largeBigInt);
        int rows = prepStatement.executeUpdate();
        assertTrue(rows == 1, "Row count doesn't match");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(
          "testSetObjectMethodWithLargeBigIntegerColumn failed with an exception message: "
              + e.getMessage());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBatchInsertWithTimestampInputFormatSet(String queryResultFormat)
      throws SQLException {
    TimeZone originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("alter session set TIMESTAMP_INPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FFTZH'");
        statement.execute(
            "create or replace table testStageBindTypes (c1 date, c2 datetime, c3 timestamp)");
        java.util.Date today = new java.util.Date();
        java.sql.Date sqldate = new java.sql.Date(today.getDate());
        java.sql.Timestamp todaySQL = new java.sql.Timestamp(today.getTime());
        try (PreparedStatement prepSt =
            connection.prepareStatement("insert into testStageBindTypes values (?, ?, ?)")) {
          for (int i = 1; i < 30000; i++) {
            prepSt.setDate(1, sqldate);
            prepSt.setDate(2, sqldate);
            prepSt.setTimestamp(3, todaySQL);
            prepSt.addBatch();
          }
          prepSt.executeBatch(); // should not throw a parsing error.
        }
      } finally {
        statement.execute("drop table if exists testStageBindTypes");
        statement.execute("alter session unset TIMESTAMP_INPUT_FORMAT");
      }
    } finally {
      TimeZone.setDefault(originalTimeZone);
    }
  }

  /**
   * Test the CALL stmt type for stored procedures. Run against test server with
   * USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS enabled.
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @Disabled
  public void testCallStatement(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.executeQuery(
            "ALTER SESSION SET USE_STATEMENT_TYPE_CALL_FOR_STORED_PROC_CALLS=true");
        statement.executeQuery(
            "create or replace procedure\n"
                + "TEST_SP_CALL_STMT_ENABLED(in1 float, in2 variant)\n"
                + "returns string language javascript as $$\n"
                + "let res = snowflake.execute({sqlText: 'select ? c1, ? c2', binds:[IN1, JSON.stringify(IN2)]});\n"
                + "res.next();\n"
                + "return res.getColumnValueAsString(1) + ' ' + res.getColumnValueAsString(2) + ' ' + IN2;\n"
                + "$$;");

        try (PreparedStatement prepStatement =
            connection.prepareStatement("call TEST_SP_CALL_STMT_ENABLED(?, to_variant(?))")) {
          prepStatement.setDouble(1, 1);
          prepStatement.setString(2, "[2,3]");

          try (ResultSet rs = prepStatement.executeQuery()) {
            String result = "1 \"[2,3]\" [2,3]";
            while (rs.next()) {
              assertEquals(result, rs.getString(1));
            }
          }
        }
      } finally {
        statement.executeQuery(
            "drop procedure if exists TEST_SP_CALL_STMT_ENABLED(float, variant)");
      }
    }
  }
}
