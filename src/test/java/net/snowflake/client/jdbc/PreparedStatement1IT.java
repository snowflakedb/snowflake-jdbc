package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class PreparedStatement1IT extends PreparedStatement0IT {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetParameterMetaData(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(updateSQL)) {
        /* All binding parameters are of type text and have null precision and scale and are not nullable. Since every
           binding parameter currently has identical properties, testing is minimal until this changes.
        */
        assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(1));
        assertThat(preparedStatement.getParameterMetaData().getParameterType(1), is(Types.VARCHAR));
        assertThat(preparedStatement.getParameterMetaData().getPrecision(1), is(0));
        assertThat(preparedStatement.getParameterMetaData().getScale(1), is(0));
        assertThat(
            preparedStatement.getParameterMetaData().isNullable(1),
            is(ParameterMetaData.parameterNoNulls));
        assertThat(preparedStatement.getParameterMetaData().getParameterTypeName(1), is("text"));
      }

      try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
        assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(6));
        assertThat(preparedStatement.getParameterMetaData().getParameterType(1), is(Types.VARCHAR));
        assertThat(preparedStatement.getParameterMetaData().getParameterTypeName(1), is("text"));
        assertThat(preparedStatement.getParameterMetaData().getParameterType(6), is(Types.VARCHAR));
        assertThat(preparedStatement.getParameterMetaData().getParameterTypeName(6), is("text"));
      }
      // test a statement with no binding parameters to ensure the count is 0
      try (PreparedStatement preparedStatement = connection.prepareStatement(selectAllSQL)) {
        assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(0));
        // try to access a binding parameter that is out of range of the list of parameters (in this
        // case, the list is
        // empty so everything is out of range) and ensure an exception is thrown
        SQLException e =
            assertThrows(
                SQLException.class,
                () -> preparedStatement.getParameterMetaData().getParameterType(3));
        assertThat(e.getErrorCode(), is(NUMERIC_VALUE_OUT_OF_RANGE.getMessageCode()));
      }
    }
  }

  /** Trigger default stage array binding threshold so that it can be run on travis */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testInsertStageArrayBind(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      connection
          .createStatement()
          .execute("create or replace table testStageArrayBind(c1 integer)");
      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into testStageArrayBind values (?)")) {

        for (int i = 0; i < 70000; i++) {
          prepStatement.setInt(1, i);
          prepStatement.addBatch();
        }
        prepStatement.executeBatch();

        try (ResultSet resultSet =
            statement.executeQuery("select * from testStageArrayBind order by c1 asc")) {
          int count = 0;
          while (resultSet.next()) {
            assertThat(resultSet.getInt(1), is(count));
            count++;
          }
        }
      }
    }
  }

  static void bindOneParamSet(
      PreparedStatement prepst, int id, double colA, float colB, String colC, long colD, short colE)
      throws SQLException {
    prepst.setInt(1, id);
    prepst.setDouble(2, colA);
    prepst.setFloat(3, colB);
    prepst.setString(4, colC);
    prepst.setLong(5, colD);
    prepst.setShort(6, colE);
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareStatementWithKeys(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      connection.createStatement().execute(createTableSQL);
      try (PreparedStatement prepStatement =
          connection.prepareStatement(insertSQL, Statement.NO_GENERATED_KEYS)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        prepStatement.executeBatch();
        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertEquals(1, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testInsertBatch(String queryResultFormat) throws SQLException {
    int[] countResult;
    try (Connection connection = getConn(queryResultFormat)) {
      connection
          .createStatement()
          .execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0"); // disable stage bind
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        countResult = prepStatement.executeBatch();
        assertEquals(1, countResult[0]);
        assertEquals(1, countResult[1]);
        assertEquals(2, prepStatement.getUpdateCount());
        assertEquals(2L, prepStatement.getLargeUpdateCount());
        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertEquals(2, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testInsertBatchStage(String queryResultFormat) throws SQLException {
    int[] countResult;
    try (Connection connection = getConn(queryResultFormat)) {
      connection
          .createStatement()
          .execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 12"); // enable stage bind
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        countResult = prepStatement.executeBatch();
        assertEquals(1, countResult[0]);
        assertEquals(1, countResult[1]);
        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertEquals(2, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testInsertBatchStageMultipleTimes(String queryResultFormat) throws SQLException {
    // using the same statement to run a query multiple times shouldn't result in duplicates
    int[] countResult;
    try (Connection connection = getConn(queryResultFormat)) {
      connection
          .createStatement()
          .execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 6"); // enable stage bind
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {

        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        countResult = prepStatement.executeBatch();
        assertEquals(1, countResult.length);
        assertEquals(1, countResult[0]);
        assertEquals(1, prepStatement.getUpdateCount());
        assertEquals(1L, prepStatement.getLargeUpdateCount());

        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        countResult = prepStatement.executeBatch();
        assertEquals(1, countResult.length);
        assertEquals(1, countResult[0]);
        assertEquals(1, prepStatement.getUpdateCount());
        assertEquals(1L, prepStatement.getLargeUpdateCount());

        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertEquals(2, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testStageBatchNull(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      int[] thresholds = {0, 6}; // disabled, enabled

      for (int threshold : thresholds) {
        statement.execute("DELETE FROM TEST_PREPST WHERE 1=1"); // clear table
        statement.execute(
            String.format(
                "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = %d", threshold));
        try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
          prepStatement.setNull(1, Types.INTEGER);
          prepStatement.setNull(2, Types.DOUBLE);
          prepStatement.setNull(3, Types.FLOAT);
          prepStatement.setNull(4, Types.VARCHAR);
          prepStatement.setNull(5, Types.NUMERIC);
          prepStatement.setNull(6, Types.INTEGER);
          prepStatement.addBatch();
          int[] countResult = prepStatement.executeBatch();
          assertEquals(1, countResult.length);
          assertEquals(1, countResult[0]);
        }

        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM TEST_PREPST")) {
          assertTrue(resultSet.next());
          String errorMessage =
              "Column should be null (" + (threshold > 0 ? "stage" : "non-stage") + ")";
          resultSet.getInt(1);
          assertTrue(resultSet.wasNull(), errorMessage);
          resultSet.getDouble(2);
          assertTrue(resultSet.wasNull(), errorMessage);
          resultSet.getFloat(3);
          assertTrue(resultSet.wasNull(), errorMessage);
          resultSet.getString(4);
          assertTrue(resultSet.wasNull(), errorMessage);
          resultSet.getLong(5);
          assertTrue(resultSet.wasNull(), errorMessage);
          resultSet.getShort(6);
          assertTrue(resultSet.wasNull(), errorMessage);
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testStageString(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      int[] thresholds = {0, 6}; // disabled, enabled
      String[] rows = {
        null, "", "\"", ",", "\n", "\r\n", "\"\"", "null", "\\\n", "\",", "\\\",\\\""
      };

      for (int threshold : thresholds) {
        statement.execute("DELETE FROM TEST_PREPST WHERE 1=1"); // clear table
        statement.execute(
            String.format(
                "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = %d", threshold));
        try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
          for (int i = 0; i < rows.length; i++) {
            bindOneParamSet(prepStatement, i, 0.0, 0.0f, rows[i], 0, (short) 0);
            prepStatement.addBatch();
          }
          prepStatement.executeBatch();

          try (ResultSet resultSet =
              statement.executeQuery("SELECT colC FROM TEST_PREPST ORDER BY id ASC")) {
            String errorMessage =
                "Strings should match (" + (threshold > 0 ? "stage" : "non-stage") + ")";
            for (String row : rows) {
              assertTrue(resultSet.next());
              assertEquals(row, resultSet.getString(1), errorMessage);
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testIncorrectTypes(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      int[] thresholds = {0, 6}; // disabled, enabled

      for (int threshold : thresholds) {
        statement.execute("DELETE FROM TEST_PREPST WHERE 1=1"); // clear table
        statement.execute(
            String.format(
                "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = %d", threshold));
        try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {

          prepStatement.setString(1, "notAnInt"); // should cause error
          prepStatement.setDouble(2, 0.0);
          prepStatement.setFloat(3, 0.0f);
          prepStatement.setString(4, "");
          prepStatement.setLong(5, 0);
          prepStatement.setShort(6, (short) 0);
          prepStatement.addBatch();

          assertThrows(SQLException.class, prepStatement::executeBatch);
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testStageBatchTimestamps(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      Timestamp tsEpoch = new Timestamp(0L);
      Timestamp tsEpochMinusOneSec = new Timestamp(-1000L); // negative epoch no fraction of seconds
      Timestamp tsPast = new Timestamp(-2208988800100L); // very large negative epoch
      Timestamp tsFuture = new Timestamp(32503680000000L); // very large positive epoch
      Timestamp tsNow = new Timestamp(System.currentTimeMillis());
      Timestamp tsArbitrary = new Timestamp(862056000000L);
      Timestamp[] timestamps =
          new Timestamp[] {tsEpochMinusOneSec, tsEpoch, tsPast, tsFuture, tsNow, tsArbitrary, null};
      final String[] tsTypes = new String[] {"TIMESTAMP_LTZ", "TIMESTAMP_NTZ"};
      int[] countResult;

      try {
        // Test that stage and non-stage bindings are consistent for each timestamp type
        for (String tsType : tsTypes) {
          statement.execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = " + tsType);
          statement.execute("ALTER SESSION SET CLIENT_TIMESTAMP_TYPE_MAPPING = " + tsType);

          statement.execute("CREATE OR REPLACE TABLE test_prepst_ts (id INTEGER, tz TIMESTAMP)");
          try (PreparedStatement prepStatement =
              connection.prepareStatement("INSERT INTO test_prepst_ts(id, tz) VALUES(?,?)")) {
            // First, run with non-stage binding
            statement.executeQuery("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
            for (int i = 0; i < timestamps.length; i++) {
              prepStatement.setInt(1, i);
              prepStatement.setTimestamp(2, timestamps[i]);
              prepStatement.addBatch();
            }
            countResult = prepStatement.executeBatch();
            for (int res : countResult) {
              assertEquals(1, res);
            }

            Timestamp[] nonStageResult = new Timestamp[timestamps.length];
            try (ResultSet rsNonStage =
                statement.executeQuery("SELECT * FROM test_prepst_ts ORDER BY id ASC")) {
              for (int i = 0; i < nonStageResult.length; i++) {
                assertTrue(rsNonStage.next());
                nonStageResult[i] = rsNonStage.getTimestamp(2);
              }
            }
            statement.execute("DELETE FROM test_prepst_ts WHERE 1=1");

            // Now, run with stage binding
            statement.execute(
                "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage
            // bind
            for (int i = 0; i < timestamps.length; i++) {
              prepStatement.setInt(1, i);
              prepStatement.setTimestamp(2, timestamps[i]);
              prepStatement.addBatch();
            }
            countResult = prepStatement.executeBatch();
            for (int res : countResult) {
              assertEquals(1, res);
            }

            Timestamp[] stageResult = new Timestamp[timestamps.length];
            try (ResultSet rsStage =
                statement.executeQuery("SELECT * FROM test_prepst_ts ORDER BY id ASC")) {
              for (int i = 0; i < stageResult.length; i++) {
                assertTrue(rsStage.next());
                stageResult[i] = rsStage.getTimestamp(2);
              }

              for (int i = 0; i < timestamps.length; i++) {
                assertEquals(
                    nonStageResult[i],
                    stageResult[i],
                    "Stage binding timestamp should match non-stage binding timestamp ("
                        + tsType
                        + ")");
              }
            }
          }
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS test_prepst_ts");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testStageBatchTimes(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      Time tMidnight = new Time(0);
      Time tNeg = new Time(-1);
      Time tPos = new Time(1);
      Time tNow = new Time(System.currentTimeMillis());
      Time tNoon = new Time(12 * 60 * 60 * 1000);
      Time[] times = new Time[] {tMidnight, tNeg, tPos, tNow, tNoon, null};
      int[] countResult;
      try {
        statement.execute("CREATE OR REPLACE TABLE test_prepst_time (id INTEGER, tod TIME)");
        try (PreparedStatement prepStatement =
            connection.prepareStatement("INSERT INTO test_prepst_time(id, tod) VALUES(?,?)")) {

          // First, run with non-stage binding
          statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
          for (int i = 0; i < times.length; i++) {
            prepStatement.setInt(1, i);
            prepStatement.setTime(2, times[i]);
            prepStatement.addBatch();
          }
          countResult = prepStatement.executeBatch();
          for (int res : countResult) {
            assertEquals(1, res);
          }

          Time[] nonStageResult = new Time[times.length];
          ResultSet rsNonStage =
              statement.executeQuery("SELECT * FROM test_prepst_time ORDER BY id ASC");
          for (int i = 0; i < nonStageResult.length; i++) {
            assertTrue(rsNonStage.next());
            nonStageResult[i] = rsNonStage.getTime(2);
          }

          statement.execute("DELETE FROM test_prepst_time WHERE 1=1");

          // Now, run with stage binding
          statement.execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage
          // bind
          for (int i = 0; i < times.length; i++) {
            prepStatement.setInt(1, i);
            prepStatement.setTime(2, times[i]);
            prepStatement.addBatch();
          }
          countResult = prepStatement.executeBatch();
          for (int res : countResult) {
            assertEquals(1, res);
          }

          Time[] stageResult = new Time[times.length];
          try (ResultSet rsStage =
              statement.executeQuery("SELECT * FROM test_prepst_time ORDER BY id ASC")) {
            for (int i = 0; i < stageResult.length; i++) {
              assertTrue(rsStage.next());
              stageResult[i] = rsStage.getTime(2);
            }

            for (int i = 0; i < times.length; i++) {
              assertEquals(
                  nonStageResult[i],
                  stageResult[i],
                  "Stage binding time should match non-stage binding time");
            }
          }
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS test_prepst_time");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testClearParameters(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.clearParameters();

        int parameterSize =
            ((SnowflakePreparedStatementV1) prepStatement).getParameterBindings().size();
        assertThat(parameterSize, is(0));

        bindOneParamSet(prepStatement, 3, 1.22, 1.2f, "hello", 12222L, (short) 1);
        prepStatement.executeUpdate();

        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertTrue(resultSet.next());
          assertEquals(3, resultSet.getInt(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testClearBatch(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();

        // clear batch should remove all batch parameters
        prepStatement.clearBatch();
        int batchSize =
            ((SnowflakePreparedStatementV1) prepStatement).getBatchParameterBindings().size();
        assertThat(batchSize, is(0));

        bindOneParamSet(prepStatement, 3, 1.22, 1.2f, "hello", 12222L, (short) 1);
        prepStatement.addBatch();
        prepStatement.executeBatch();

        // executeBatch should remove batch as well
        batchSize =
            ((SnowflakePreparedStatementV1) prepStatement).getBatchParameterBindings().size();
        assertThat(batchSize, is(0));

        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertTrue(resultSet.next());
          assertEquals(3, resultSet.getInt(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testInsertOneRow(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE test_prepst_date (id INTEGER, d DATE)");
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        assertEquals(1, prepStatement.executeUpdate());
      }
      try (ResultSet resultSet = statement.executeQuery(selectAllSQL)) {
        assertEquals(1, getSizeOfResultSet(resultSet));
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        assertFalse(prepStatement.execute());
        assertEquals(1, prepStatement.getUpdateCount());
        assertEquals(1L, prepStatement.getLargeUpdateCount());
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testUpdateOneRow(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE test_prepst_date (id INTEGER, d DATE)");
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        prepStatement.executeBatch();
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(updateSQL)) {
        prepStatement.setInt(1, 1);
        int count = prepStatement.executeUpdate();
        assertEquals(1, count);
        try (ResultSet resultSet = statement.executeQuery(selectAllSQL)) {
          assertTrue(resultSet.next());
          assertEquals("newString", resultSet.getString(4));
        }
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(updateSQL)) {
        prepStatement.setInt(1, 2);
        assertFalse(prepStatement.execute());
        assertEquals(1, prepStatement.getUpdateCount());
        assertEquals(1L, prepStatement.getLargeUpdateCount());
        try (ResultSet resultSet = statement.executeQuery(selectAllSQL)) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.next());
          assertEquals("newString", resultSet.getString(4));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDeleteOneRow(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE OR REPLACE TABLE test_prepst_date (id INTEGER, d DATE)");
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        prepStatement.executeBatch();
      }
      String qid1;
      try (PreparedStatement prepStatement = connection.prepareStatement(deleteSQL)) {
        prepStatement.setInt(1, 1);
        int count = prepStatement.executeUpdate();
        assertEquals(1, count);
        try (ResultSet resultSet = statement.executeQuery(selectAllSQL)) {
          assertEquals(1, getSizeOfResultSet(resultSet));
        }
        // evaluate query ids
        assertTrue(prepStatement.isWrapperFor(SnowflakePreparedStatement.class));
        qid1 = prepStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
        assertNotNull(qid1);
      }

      try (PreparedStatement prepStatement = connection.prepareStatement(deleteSQL)) {
        prepStatement.setInt(1, 2);
        assertFalse(prepStatement.execute());
        assertEquals(1, prepStatement.getUpdateCount());
        assertEquals(1L, prepStatement.getLargeUpdateCount());
        try (ResultSet resultSet = statement.executeQuery(selectAllSQL)) {
          assertEquals(0, getSizeOfResultSet(resultSet));
          // evaluate query ids
          assertTrue(prepStatement.isWrapperFor(SnowflakePreparedStatement.class));
          String qid2 = prepStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
          assertNotNull(qid2);
          assertNotEquals(qid1, qid2);
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSelectOneRow(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        prepStatement.executeBatch();
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(selectSQL)) {
        prepStatement.setInt(1, 2);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertEquals(1, getSizeOfResultSet(resultSet));
        }
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(selectSQL)) {
        prepStatement.setInt(1, 2);
        assertTrue(prepStatement.execute());
        try (ResultSet resultSet = prepStatement.getResultSet()) {
          assertEquals(1, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testUpdateBatch(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
        prepStatement.addBatch();
        prepStatement.executeBatch();
      }

      try (PreparedStatement prepStatement = connection.prepareStatement(updateSQL)) {
        prepStatement.setInt(1, 1);
        prepStatement.addBatch();
        prepStatement.setInt(1, 2);
        prepStatement.addBatch();
        prepStatement.setInt(1, 3);
        prepStatement.addBatch();

        int[] counts = prepStatement.executeBatch();
        assertThat(counts[0], is(1));
        assertThat(counts[1], is(1));
        assertThat(counts[2], is(0));
        assertEquals(0, prepStatement.getUpdateCount());
        assertEquals(0L, prepStatement.getLargeUpdateCount());
        try (ResultSet resultSet = connection.createStatement().executeQuery(selectAllSQL)) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getString(4), is("newString"));
          assertTrue(resultSet.next());
          assertThat(resultSet.getString(4), is("newString"));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBatchInsertWithCacheEnabled(String queryResultFormat) throws SQLException {
    int[] countResult;
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      // ensure enable the cache result use
      statement.execute(enableCacheReuse);

      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 1);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 2);
        prepStatement.addBatch();
        countResult = prepStatement.executeBatch();
        assertEquals(1, countResult[0]);
        assertEquals(1, countResult[1]);

        prepStatement.clearBatch();
        bindOneParamSet(prepStatement, 3, 3.3333, (float) 3.2, "test3", 1221221123131L, (short) 3);
        prepStatement.addBatch();
        bindOneParamSet(prepStatement, 4, 4.4444, (float) 4.2, "test4", 1221221123131L, (short) 4);
        prepStatement.addBatch();
        countResult = prepStatement.executeBatch();
        assertEquals(1, countResult[0]);
        assertEquals(1, countResult[1]);

        try (ResultSet resultSet = statement.executeQuery(selectAllSQL)) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
          assertTrue(resultSet.next());
          assertEquals(2, resultSet.getInt(1));
          assertTrue(resultSet.next());
          assertEquals(3, resultSet.getInt(1));
          assertTrue(resultSet.next());
          assertEquals(4, resultSet.getInt(1));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  /**
   * Manual test to ensure proper log file is produced when
   * CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS is enabled. Look in /tmp folder for
   * snowflake_jdbc0.log.0 and check that it lists binding params.
   *
   * @throws SQLException arises if any exception occurs
   */
  @Test
  @Disabled
  public void manualTestForPreparedStatementLogging() throws SQLException {
    Map<String, String> params = getConnectionParameters();
    Properties props = new Properties();
    String uri = params.get("uri");
    props.put("account", params.get("account"));
    props.put("ssl", params.get("ssl"));
    props.put("database", params.get("database"));
    props.put("schema", params.get("schema"));
    props.put("user", params.get("user"));
    props.put("password", params.get("password"));
    props.put("tracing", "info");
    try (Connection con = DriverManager.getConnection(uri, props);
        Statement statement = con.createStatement()) {
      statement.executeUpdate("alter session set CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS=true");
      statement.execute(createTableSQL);
      try (PreparedStatement prepStatement =
          con.prepareStatement(insertSQL, Statement.NO_GENERATED_KEYS)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.addBatch();
        prepStatement.executeBatch();
        statement.executeUpdate(
            "alter session set CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS=false");
      }
    }
  }
}
