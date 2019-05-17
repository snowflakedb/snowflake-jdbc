/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.google.common.collect.Sets;
import net.snowflake.client.ConditionalIgnoreRule.ConditionalIgnore;
import net.snowflake.client.RunningOnTravisCI;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Set;

import static net.snowflake.client.jdbc.ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Prepared statement integration tests
 */
public class PreparedStatementIT extends BaseJDBCTest
{
  private Connection connection = null;
  private PreparedStatement prepStatement = null;
  private Statement statement = null;
  private ResultSet resultSet = null;
  private final String insertSQL = "insert into TEST_PREPST values(?, ?, ?, ?, ?, ?)";
  private final String selectAllSQL = "select * from TEST_PREPST";
  private final String updateSQL = "update TEST_PREPST set COLC = 'newString' where ID = ?";
  private final String deleteSQL = "delete from TEST_PREPST where ID = ?";
  private final String selectSQL = "select * from TEST_PREPST where ID = ?";
  private final String createTableSQL = "create or replace table test_prepst(id INTEGER, "
                                        + "colA DOUBLE, colB FLOAT, colC String,  "
                                        + "colD NUMBER, col INTEGER)";
  private final String deleteTableSQL = "drop table if exists TEST_PREPST";
  private final String enableCacheReuse = "alter session set USE_CACHED_RESULT=true";
  @Rule
  public final ExpectedException exception = ExpectedException.none();


  @Before
  public void setUp() throws SQLException
  {
    Connection con = getConnection();
    Statement st = con.createStatement();
    st.execute(createTableSQL);
    con.close();
  }

  @After
  public void tearDown() throws SQLException
  {
    Connection con = getConnection();
    Statement st = con.createStatement();
    st.execute(deleteTableSQL);
    con.close();
  }

  @Test
  public void testGetParameterMetaData() throws SQLException
  {
    connection = getConnection();
    statement = connection.createStatement();
    PreparedStatement preparedStatement = connection.prepareStatement(updateSQL);
    /* All binding parameters are of type text and have null precision and scale and are not nullable. Since every
    binding parameter currently has identical properties, testing is minimal until this changes.
     */
    assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(1));
    assertThat(preparedStatement.getParameterMetaData().getParameterType(1), is(Types.VARCHAR));
    assertThat(preparedStatement.getParameterMetaData().getPrecision(1), is(0));
    assertThat(preparedStatement.getParameterMetaData().getScale(1), is(0));
    assertThat(preparedStatement.getParameterMetaData().isNullable(1), is(ParameterMetaData.parameterNoNulls));
    assertThat(preparedStatement.getParameterMetaData().getParameterTypeName(1), is("text"));

    preparedStatement = connection.prepareStatement(insertSQL);
    assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(6));
    assertThat(preparedStatement.getParameterMetaData().getParameterType(1), is(Types.VARCHAR));
    assertThat(preparedStatement.getParameterMetaData().getParameterTypeName(1), is("text"));
    assertThat(preparedStatement.getParameterMetaData().getParameterType(6), is(Types.VARCHAR));
    assertThat(preparedStatement.getParameterMetaData().getParameterTypeName(6), is("text"));
    // test a statement with no binding parameters to ensure the count is 0
    preparedStatement = connection.prepareStatement(selectAllSQL);
    assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(0));
    // try to access a binding parameter that is out of range of the list of parameters (in this case, the list is
    // empty so everything is out of range) and ensure an exception is thrown
    try
    {
      preparedStatement.getParameterMetaData().getParameterType(3);
      fail("An exception should have been thrown");
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(NUMERIC_VALUE_OUT_OF_RANGE.getMessageCode()));
    }
  }

  /**
   * Trigger default stage array binding threshold so that it can be run
   * on travis
   */
  @Test
  public void testInsertStageArrayBind() throws SQLException
  {
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("create or replace table testStageArrayBind(c1 integer)");

    prepStatement = connection.prepareStatement(
        "insert into testStageArrayBind values (?)");

    for (int i = 0; i < 70000; i++)
    {
      prepStatement.setInt(1, i);
      prepStatement.addBatch();
    }
    prepStatement.executeBatch();

    resultSet = statement.executeQuery("select * from testStageArrayBind " +
                                       "order by c1 asc");

    int count = 0;
    while (resultSet.next())
    {
      assertThat(resultSet.getInt(1), is(count));

      count++;
    }

    resultSet.close();
    statement.close();
    prepStatement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testInsertBatch() throws SQLException
  {
    int[] countResult;
    connection = getConnection();
    connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0"); // disable stage bind
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    countResult = prepStatement.executeBatch();
    assertEquals(1, countResult[0]);
    assertEquals(1, countResult[1]);
    assertEquals(2, prepStatement.getUpdateCount());
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    assertEquals(2, getSizeOfResultSet(resultSet));
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testInsertBatchStage() throws SQLException
  {
    int[] countResult;
    connection = getConnection();
    connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 12"); // enable stage bind
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    countResult = prepStatement.executeBatch();
    assertEquals(1, countResult[0]);
    assertEquals(1, countResult[1]);
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    assertEquals(2, getSizeOfResultSet(resultSet));
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testInsertBatchStageMultipleTimes() throws SQLException
  {
    // using the same statement to run a query multiple times shouldn't result in duplicates
    int[] countResult;
    connection = getConnection();
    connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 6"); // enable stage bind
    prepStatement = connection.prepareStatement(insertSQL);

    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    countResult = prepStatement.executeBatch();
    assertEquals(1, countResult.length);
    assertEquals(1, countResult[0]);
    assertEquals(1, prepStatement.getUpdateCount());

    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    countResult = prepStatement.executeBatch();
    assertEquals(1, countResult.length);
    assertEquals(1, countResult[0]);
    assertEquals(1, prepStatement.getUpdateCount());

    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    assertEquals(2, getSizeOfResultSet(resultSet));
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testStageBatchNull() throws SQLException
  {
    int[] countResult;
    connection = getConnection();
    int[] thresholds = {0, 6}; // disabled, enabled

    for (int threshold : thresholds)
    {
      connection.createStatement().execute("DELETE FROM TEST_PREPST WHERE 1=1"); // clear table
      connection.createStatement().execute(
          String.format("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = %d", threshold));
      prepStatement = connection.prepareStatement(insertSQL);
      prepStatement.setNull(1, Types.INTEGER);
      prepStatement.setNull(2, Types.DOUBLE);
      prepStatement.setNull(3, Types.FLOAT);
      prepStatement.setNull(4, Types.VARCHAR);
      prepStatement.setNull(5, Types.NUMERIC);
      prepStatement.setNull(6, Types.INTEGER);
      prepStatement.addBatch();
      countResult = prepStatement.executeBatch();
      assertEquals(1, countResult.length);
      assertEquals(1, countResult[0]);

      resultSet = connection.createStatement().executeQuery("SELECT * FROM TEST_PREPST");
      resultSet.next();

      String errorMessage = "Column should be null (" + (threshold > 0 ? "stage" : "non-stage") + ")";
      resultSet.getInt(1);
      assertTrue(errorMessage, resultSet.wasNull());
      resultSet.getDouble(2);
      assertTrue(errorMessage, resultSet.wasNull());
      resultSet.getFloat(3);
      assertTrue(errorMessage, resultSet.wasNull());
      resultSet.getString(4);
      assertTrue(errorMessage, resultSet.wasNull());
      resultSet.getLong(5);
      assertTrue(errorMessage, resultSet.wasNull());
      resultSet.getShort(6);
      assertTrue(errorMessage, resultSet.wasNull());
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testStageString() throws SQLException
  {
    connection = getConnection();
    int[] thresholds = {0, 6}; // disabled, enabled
    String[] rows = {null, "", "\"", ",", "\n", "\r\n", "\"\"", "null", "\\\n", "\",", "\\\",\\\""};

    for (int threshold : thresholds)
    {
      connection.createStatement().execute("DELETE FROM TEST_PREPST WHERE 1=1"); // clear table
      connection.createStatement().execute(
          String.format("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = %d", threshold));
      prepStatement = connection.prepareStatement(insertSQL);
      for (int i = 0; i < rows.length; i++)
      {
        bindOneParamSet(prepStatement, i, 0.0, 0.0f, rows[i], 0, (short) 0);
        prepStatement.addBatch();
      }
      prepStatement.executeBatch();

      resultSet = connection.createStatement().executeQuery("SELECT colC FROM TEST_PREPST ORDER BY id ASC");
      String errorMessage = "Strings should match (" + (threshold > 0 ? "stage" : "non-stage") + ")";

      for (int i = 0; i < rows.length; i++)
      {
        resultSet.next();
        assertEquals(errorMessage, rows[i], resultSet.getString(1));
      }
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testIncorrectTypes() throws SQLException
  {
    connection = getConnection();
    int[] thresholds = {0, 6}; // disabled, enabled

    for (int threshold : thresholds)
    {
      connection.createStatement().execute("DELETE FROM TEST_PREPST WHERE 1=1"); // clear table
      connection.createStatement().execute(
          String.format("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = %d", threshold));
      prepStatement = connection.prepareStatement(insertSQL);

      prepStatement.setString(1, "notAnInt"); // should cause error
      prepStatement.setDouble(2, 0.0);
      prepStatement.setFloat(3, 0.0f);
      prepStatement.setString(4, "");
      prepStatement.setLong(5, 0);
      prepStatement.setShort(6, (short) 0);
      prepStatement.addBatch();

      try
      {
        prepStatement.executeBatch();
        fail("An exception should have been thrown");
      }
      catch (SQLException ex)
      {
      }
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testStageBatchTimestamps() throws SQLException
  {
    connection = getConnection();
    Timestamp tsEpoch = new Timestamp(0L);
    Timestamp tsEpochMinusOneSec = new Timestamp(-1000L); // negative epoch no fraction of seconds
    Timestamp tsPast = new Timestamp(-2208988800100L); // very large negative epoch
    Timestamp tsFuture = new Timestamp(32503680000000L); // very large positive epoch
    Timestamp tsNow = new Timestamp(System.currentTimeMillis());
    Timestamp tsArbitrary = new Timestamp(862056000000L);
    Timestamp[] timestamps = new Timestamp[]{
        tsEpochMinusOneSec,
        tsEpoch,
        tsPast,
        tsFuture,
        tsNow,
        tsArbitrary,
        null};
    final String[] tsTypes = new String[]{"TIMESTAMP_LTZ", "TIMESTAMP_NTZ"};
    int[] countResult;

    try
    {
      // Test that stage and non-stage bindings are consistent for each timestamp type
      for (String tsType : tsTypes)
      {
        connection.createStatement().execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = " + tsType);
        connection.createStatement().execute("ALTER SESSION SET CLIENT_TIMESTAMP_TYPE_MAPPING = " + tsType);

        connection.createStatement().execute("CREATE OR REPLACE TABLE test_prepst_ts (id INTEGER, tz TIMESTAMP)");
        prepStatement = connection.prepareStatement("INSERT INTO test_prepst_ts(id, tz) VALUES(?,?)");

        // First, run with non-stage binding
        connection.createStatement().executeQuery("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
        for (int i = 0; i < timestamps.length; i++)
        {
          prepStatement.setInt(1, i);
          prepStatement.setTimestamp(2, timestamps[i]);
          prepStatement.addBatch();
        }
        countResult = prepStatement.executeBatch();
        for (int res : countResult)
        {
          assertEquals(1, res);
        }

        Timestamp[] nonStageResult = new Timestamp[timestamps.length];
        ResultSet rsNonStage =
            connection.createStatement().executeQuery("SELECT * FROM test_prepst_ts ORDER BY id ASC");
        for (int i = 0; i < nonStageResult.length; i++)
        {
          rsNonStage.next();
          nonStageResult[i] = rsNonStage.getTimestamp(2);
        }

        connection.createStatement().execute("DELETE FROM test_prepst_ts WHERE 1=1");

        // Now, run with stage binding
        connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
        for (int i = 0; i < timestamps.length; i++)
        {
          prepStatement.setInt(1, i);
          prepStatement.setTimestamp(2, timestamps[i]);
          prepStatement.addBatch();
        }
        countResult = prepStatement.executeBatch();
        for (int res : countResult)
        {
          assertEquals(1, res);
        }

        Timestamp[] stageResult = new Timestamp[timestamps.length];
        ResultSet rsStage = connection.createStatement().executeQuery("SELECT * FROM test_prepst_ts ORDER BY id ASC");
        for (int i = 0; i < stageResult.length; i++)
        {
          rsStage.next();
          stageResult[i] = rsStage.getTimestamp(2);
        }

        for (int i = 0; i < timestamps.length; i++)
        {
          assertEquals("Stage binding timestamp should match non-stage binding timestamp (" + tsType + ")",
                       nonStageResult[i], stageResult[i]);
        }
      }

    }
    finally
    {
      connection.createStatement().execute("DROP TABLE IF EXISTS test_prepst_ts");
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testStageBatchTimes() throws SQLException
  {
    connection = getConnection();
    Time tMidnight = new Time(0);
    Time tNeg = new Time(-1);
    Time tPos = new Time(1);
    Time tNow = new Time(System.currentTimeMillis());
    Time tNoon = new Time(12 * 60 * 60 * 1000);
    Time[] times = new Time[]{
        tMidnight,
        tNeg,
        tPos,
        tNow,
        tNoon,
        null
    };
    int[] countResult;
    try
    {
      connection.createStatement().execute("CREATE OR REPLACE TABLE test_prepst_time (id INTEGER, tod TIME)");
      prepStatement = connection.prepareStatement("INSERT INTO test_prepst_time(id, tod) VALUES(?,?)");

      // First, run with non-stage binding
      connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
      for (int i = 0; i < times.length; i++)
      {
        prepStatement.setInt(1, i);
        prepStatement.setTime(2, times[i]);
        prepStatement.addBatch();
      }
      countResult = prepStatement.executeBatch();
      for (int res : countResult)
      {
        assertEquals(1, res);
      }

      Time[] nonStageResult = new Time[times.length];
      ResultSet rsNonStage =
          connection.createStatement().executeQuery("SELECT * FROM test_prepst_time ORDER BY id ASC");
      for (int i = 0; i < nonStageResult.length; i++)
      {
        rsNonStage.next();
        nonStageResult[i] = rsNonStage.getTime(2);
      }

      connection.createStatement().execute("DELETE FROM test_prepst_time WHERE 1=1");

      // Now, run with stage binding
      connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
      for (int i = 0; i < times.length; i++)
      {
        prepStatement.setInt(1, i);
        prepStatement.setTime(2, times[i]);
        prepStatement.addBatch();
      }
      countResult = prepStatement.executeBatch();
      for (int res : countResult)
      {
        assertEquals(1, res);
      }

      Time[] stageResult = new Time[times.length];
      ResultSet rsStage = connection.createStatement().executeQuery("SELECT * FROM test_prepst_time ORDER BY id ASC");
      for (int i = 0; i < stageResult.length; i++)
      {
        rsStage.next();
        stageResult[i] = rsStage.getTime(2);
      }

      for (int i = 0; i < times.length; i++)
      {
        assertEquals("Stage binding time should match non-stage binding time",
                     nonStageResult[i], stageResult[i]);
      }

    }
    finally
    {
      connection.createStatement().execute("DROP TABLE IF EXISTS test_prepst_time");
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testStageBatchDates() throws SQLException
  {
    connection = getConnection();
    Date dEpoch = new Date(0);
    Date dAfterEpoch = new Date(24 * 60 * 60 * 1000);
    Date dBeforeEpoch = new Date(-1 * 24 * 60 * 60 * 1000);
    Date dNow = new Date(System.currentTimeMillis());
    Date dLeapYear = new Date(951782400000L);
    Date dFuture = new Date(32503680000000L);
    Date[] dates = new Date[]{dEpoch, dAfterEpoch, dBeforeEpoch, dNow, dLeapYear, dFuture};
    int[] countResult;

    try
    {
      connection.createStatement().execute("CREATE OR REPLACE TABLE test_prepst_date (id INTEGER, d DATE)");
      prepStatement = connection.prepareStatement("INSERT INTO test_prepst_date(id, d) VALUES(?,?)");

      // First, run with non-stage binding
      connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
      for (int i = 0; i < dates.length; i++)
      {
        prepStatement.setInt(1, i);
        prepStatement.setDate(2, dates[i]);
        prepStatement.addBatch();
      }
      countResult = prepStatement.executeBatch();
      for (int res : countResult)
      {
        assertEquals(1, res);
      }

      Date[] nonStageResult = new Date[dates.length];
      ResultSet rsNonStage =
          connection.createStatement().executeQuery("SELECT * FROM test_prepst_date ORDER BY id ASC");
      for (int i = 0; i < nonStageResult.length; i++)
      {
        rsNonStage.next();
        nonStageResult[i] = rsNonStage.getDate(2);
      }

      connection.createStatement().execute("DELETE FROM test_prepst_date WHERE 1=1");

      // Now, run with stage binding
      connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
      for (int i = 0; i < dates.length; i++)
      {
        prepStatement.setInt(1, i);
        prepStatement.setDate(2, dates[i]);
        prepStatement.addBatch();
      }
      countResult = prepStatement.executeBatch();
      for (int res : countResult)
      {
        assertEquals(1, res);
      }

      Date[] stageResult = new Date[dates.length];
      ResultSet rsStage = connection.createStatement().executeQuery("SELECT * FROM test_prepst_date ORDER BY id ASC");
      for (int i = 0; i < stageResult.length; i++)
      {
        rsStage.next();
        stageResult[i] = rsStage.getDate(2);
      }

      for (int i = 0; i < dates.length; i++)
      {
        assertEquals("Stage binding date should match non-stage binding date",
                     nonStageResult[i], stageResult[i]);
      }

    }
    finally
    {
      connection.createStatement().execute("DROP TABLE IF EXISTS test_prepst_date");
    }
  }


  @Test
  public void testClearParameters() throws SQLException
  {
    connection = getConnection();
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.clearParameters();

    int parameterSize =
        ((SnowflakePreparedStatementV1) prepStatement).getParameterBindings().size();
    assertThat(parameterSize, is(0));

    bindOneParamSet(prepStatement, 3, 1.22, 1.2f, "hello", 12222L, (short) 1);
    prepStatement.executeUpdate();

    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    resultSet.next();
    assertEquals(3, resultSet.getInt(1));
    assertTrue(!resultSet.next());
    connection.close();
  }

  @Test
  public void testClearBatch() throws SQLException
  {
    connection = getConnection();
    prepStatement = connection.prepareStatement(insertSQL);
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

    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    resultSet.next();
    assertEquals(3, resultSet.getInt(1));
    assertTrue(!resultSet.next());
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testInsertOneRow() throws SQLException
  {
    int count = 0;
    connection = getConnection();
    connection.createStatement().execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    count = prepStatement.executeUpdate();
    assertEquals(1, count);
    prepStatement.close();
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    assertEquals(1, getSizeOfResultSet(resultSet));

    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    assertTrue(!prepStatement.execute());
    count = prepStatement.getUpdateCount();
    assertEquals(1, count);

    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testUpdateOneRow() throws SQLException
  {
    connection = getConnection();
    connection.createStatement().execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    prepStatement.executeBatch();
    prepStatement.close();

    prepStatement = connection.prepareStatement(updateSQL);
    prepStatement.setInt(1, 1);
    int count = prepStatement.executeUpdate();
    assertEquals(1, count);
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    resultSet.next();
    assertEquals("newString", resultSet.getString(4));
    resultSet.close();

    prepStatement = connection.prepareStatement(updateSQL);
    prepStatement.setInt(1, 2);
    assertTrue(!prepStatement.execute());
    assertEquals(1, prepStatement.getUpdateCount());
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    resultSet.next();
    resultSet.next();
    assertEquals("newString", resultSet.getString(4));

    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testDeleteOneRow() throws SQLException
  {
    connection = getConnection();
    connection.createStatement().execute("alter session set JDBC_EXECUTE_RETURN_COUNT_FOR_DML = true");
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    prepStatement.executeBatch();
    prepStatement.close();

    prepStatement = connection.prepareStatement(deleteSQL);
    prepStatement.setInt(1, 1);
    int count = prepStatement.executeUpdate();
    assertEquals(1, count);
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    assertEquals(1, getSizeOfResultSet(resultSet));
    // evaluate query ids
    assertTrue(prepStatement.isWrapperFor(SnowflakePreparedStatement.class));
    String qid1 = prepStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
    assertNotNull(qid1);

    prepStatement = connection.prepareStatement(deleteSQL);
    prepStatement.setInt(1, 2);
    assertTrue(!prepStatement.execute());
    assertEquals(1, prepStatement.getUpdateCount());
    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    assertEquals(0, getSizeOfResultSet(resultSet));
    // evaluate query ids
    assertTrue(prepStatement.isWrapperFor(SnowflakePreparedStatement.class));
    String qid2 = prepStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
    assertNotNull(qid2);
    assertNotEquals(qid1, qid2);
    resultSet.close();
  }

  @Test
  public void testSelectOneRow() throws SQLException
  {
    boolean isResultSet;
    connection = getConnection();
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    prepStatement.executeBatch();
    prepStatement.close();

    prepStatement = connection.prepareStatement(selectSQL);
    prepStatement.setInt(1, 2);
    resultSet = prepStatement.executeQuery();
    assertEquals(1, getSizeOfResultSet(resultSet));

    prepStatement = connection.prepareStatement(selectSQL);
    prepStatement.setInt(1, 2);
    assertTrue(prepStatement.execute());
    resultSet = prepStatement.getResultSet();
    assertEquals(1, getSizeOfResultSet(resultSet));
    connection.close();
  }

  @Test
  public void testUpdateBatch() throws SQLException
  {
    connection = getConnection();
    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.addBatch();
    bindOneParamSet(prepStatement, 2, 2.22222, (float) 2.2, "test2", 1221221123131L, (short) 1);
    prepStatement.addBatch();
    prepStatement.executeBatch();
    prepStatement.close();

    prepStatement = connection.prepareStatement(updateSQL);
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
    resultSet = prepStatement.getResultSet();
    assertEquals(0, prepStatement.getUpdateCount());

    resultSet = connection.createStatement().executeQuery(selectAllSQL);
    resultSet.next();
    assertThat(resultSet.getString(4), is("newString"));
    resultSet.next();
    assertThat(resultSet.getString(4), is("newString"));

    connection.close();
  }

  @Test
  public void testLimitBind() throws SQLException
  {
    connection = getConnection();
    String stmtStr = "select seq4() from table(generator(rowcount=>100)) " +
                     "limit ?";

    prepStatement = connection.prepareStatement(stmtStr);
    prepStatement.setInt(1, 10);
    resultSet = prepStatement.executeQuery();

    connection.close();
  }

  /**
   * SNOW-31746
   */
  @Test
  public void testConstOptLimitBind() throws SQLException
  {
    connection = getConnection();
    String stmtStr = "select 1 limit ? offset ?";

    prepStatement = connection.prepareStatement(stmtStr);
    prepStatement.setInt(1, 10);
    prepStatement.setInt(2, 0);
    resultSet = prepStatement.executeQuery();
    resultSet.next();
    assertThat(resultSet.getInt(1), is(1));

    assertThat(resultSet.next(), is(false));
    connection.close();
  }

  @Test
  public void testPrepStWithCacheEnabled() throws SQLException
  {
    connection = getConnection();
    // ensure enable the cache result use
    statement = connection.createStatement();
    statement.execute(enableCacheReuse);

    prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.execute();
    prepStatement.execute();
    bindOneParamSet(prepStatement, 100, 1.2222, (float) 1.2, "testA", 12122L, (short) 12);
    prepStatement.execute();
    statement = connection.createStatement();
    resultSet = statement.executeQuery("select * from test_prepst");
    resultSet.next();
    assertEquals(resultSet.getInt(1), 1);
    resultSet.next();
    assertEquals(resultSet.getInt(1), 1);
    resultSet.next();
    assertEquals(resultSet.getInt(1), 100);

    prepStatement = connection.prepareStatement("select id, id + ? from test_prepst where id = ?");
    prepStatement.setInt(1, 1);
    prepStatement.setInt(2, 1);
    resultSet = prepStatement.executeQuery();
    resultSet.next();
    assertEquals(resultSet.getInt(2), 2);
    prepStatement.setInt(1, 1);
    prepStatement.setInt(2, 100);
    resultSet = prepStatement.executeQuery();
    resultSet.next();
    assertEquals(resultSet.getInt(2), 101);

    prepStatement = connection.prepareStatement(
        "select seq4() from table(generator(rowcount=>100)) limit ?");
    prepStatement.setInt(1, 1);
    resultSet = prepStatement.executeQuery();
    assertTrue(resultSet.next());
    assertTrue(!resultSet.next());
    prepStatement.setInt(1, 3);
    resultSet = prepStatement.executeQuery();
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertTrue(!resultSet.next());

    connection.close();
  }

  @Test
  public void testBatchInsertWithCacheEnabled() throws SQLException
  {
    int[] countResult;
    connection = getConnection();
    // ensure enable the cache result use
    statement = connection.createStatement();
    statement.execute(enableCacheReuse);

    prepStatement = connection.prepareStatement(insertSQL);
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

    resultSet = statement.executeQuery(selectAllSQL);
    resultSet.next();
    assertEquals(1, resultSet.getInt(1));
    resultSet.next();
    assertEquals(2, resultSet.getInt(1));
    resultSet.next();
    assertEquals(3, resultSet.getInt(1));
    resultSet.next();
    assertEquals(4, resultSet.getInt(1));
    assertTrue(!resultSet.next());

    connection.close();
  }

  @Test
  public void testBindWithNullValue() throws SQLException
  {
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("create or replace table testBindNull(cola date, colb time, colc timestamp, cold number)");

    prepStatement = connection.prepareStatement("insert into testBindNull values (?, ?, ?, ?)");
    prepStatement.setDate(1, null);
    prepStatement.setTime(2, null);
    prepStatement.setTimestamp(3, null);
    prepStatement.setBigDecimal(4, null);

    prepStatement.addBatch();
    prepStatement.executeBatch();

    resultSet = statement.executeQuery("select * from testBindNull");
    resultSet.next();
    Date date = resultSet.getDate(1);
    assertNull(date);
    assertTrue(resultSet.wasNull());

    Time time = resultSet.getTime(2);
    assertNull(time);
    assertTrue(resultSet.wasNull());

    Timestamp timestamp = resultSet.getTimestamp(3);
    assertNull(timestamp);
    assertTrue(resultSet.wasNull());

    BigDecimal bg = resultSet.getBigDecimal(4);
    assertNull(bg);
    assertTrue(resultSet.wasNull());

    statement.execute("TRUNCATE table testbindnull");

    prepStatement.setDate(1, null, Calendar.getInstance());
    prepStatement.setTime(2, null, Calendar.getInstance());
    prepStatement.setTimestamp(3, null, Calendar.getInstance());
    prepStatement.setBigDecimal(4, null);

    prepStatement.addBatch();
    prepStatement.executeBatch();

    resultSet = statement.executeQuery("select * from testBindNull");
    resultSet.next();
    date = resultSet.getDate(1);
    assertNull(date);
    assertTrue(resultSet.wasNull());

    time = resultSet.getTime(2);
    assertNull(time);
    assertTrue(resultSet.wasNull());

    timestamp = resultSet.getTimestamp(3);
    assertNull(timestamp);
    assertTrue(resultSet.wasNull());

    connection.close();
  }

  @Test
  public void testPrepareDDL() throws SQLException
  {
    connection = getConnection();
    try
    {
      statement = connection.createStatement();
      prepStatement = connection.prepareStatement(
          "create or replace table testprepareddl(cola number)");

      prepStatement.execute();

      resultSet = statement.executeQuery("show tables like 'testprepareddl'");

      // result should only have one row since table is created
      assertThat(resultSet.next(), is(true));
      assertThat(resultSet.next(), is(false));
    }
    finally
    {
      connection.createStatement().execute("drop table if exists testprepareddl");
      connection.close();
    }
  }

  @Test
  public void testPrepareSCL() throws SQLException
  {
    connection = getConnection();
    try
    {
      prepStatement = connection.prepareStatement("use SCHEMA  PUBLIC");
      prepStatement.execute();

      statement = connection.createStatement();
      resultSet = statement.executeQuery("select current_schema()");

      assertThat(resultSet.next(), is(true));
      assertThat(resultSet.getString(1), is("PUBLIC"));
    }
    finally
    {
      connection.close();
    }
  }

  @Test
  public void testPrepareTCL() throws SQLException
  {
    try
    {
      connection = getConnection();
      connection.setAutoCommit(false);

      String[] testCases = {"BEGIN", "COMMIT"};

      for (String testCase : testCases)
      {
        prepStatement = connection.prepareStatement(testCase);
        resultSet = prepStatement.executeQuery();

        resultSet.next();
        assertThat(resultSet.getString(1), is("Statement executed successfully."));
      }
    }
    finally
    {
      connection.close();
    }
  }

  @Test
  public void testPrepareShowCommand() throws SQLException
  {
    try
    {
      connection = getConnection();

      prepStatement = connection.prepareStatement("show databases");
      resultSet = prepStatement.executeQuery();

      assertThat(resultSet.next(), is(true));

    }
    finally
    {
      connection.close();
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testCombineDescribe() throws SQLException
  {
    Connection adminCon = getSnowflakeAdminConnection();
    adminCon.createStatement().execute(
        "alter system set enable_combined_describe=true");

    try
    {
      connection = getConnection();

      connection.createStatement().execute(
          "alter session set jdbc_enable_combined_describe=true");

      prepStatement = connection.prepareStatement(
          "select c1 from orders order by c1");

      ResultSet resultSet = prepStatement.executeQuery();

      resultSet.next();

      assertThat(resultSet.getString(1), is("1"));
      connection.close();
    }
    finally
    {
      adminCon.createStatement().execute(
          "alter system set enable_combined_describe=default");
      adminCon.close();
    }
  }

  /**
   * This test simulates the prepare timeout. When combine
   * describe and execute, combine thread will wait max to
   * 1 seconds before client coming back with execution request.
   * Sleep thread for 5 second will leads to gs recompiling the statement.
   *
   * @throws SQLException         Will be thrown if any of driver calls fail
   * @throws InterruptedException Will be thrown if the sleep is interrupted
   */
  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testPrepareTimeout() throws SQLException, InterruptedException
  {
    Connection adminCon = getSnowflakeAdminConnection();
    adminCon.createStatement().execute(
        "alter system set enable_combined_describe=true");

    try
    {
      connection = getConnection();

      connection.createStatement().execute(
          "alter session set jdbc_enable_combined_describe=true");

      prepStatement = connection.prepareStatement(
          "select c1 from orders order by c1 limit 1");

      Thread.sleep(5000);

      resultSet = prepStatement.executeQuery();

      resultSet.next();

      assertThat(resultSet.getInt(1), is(1));

      connection.close();
    }
    finally
    {
      adminCon.createStatement().execute(
          "alter system set enable_combined_describe=default");
      adminCon.close();
    }
  }

  @Test
  public void testCompilationErrorWhenPrepare() throws SQLException
  {
    connection = getConnection();

    try
    {
      connection.prepareStatement("select * from table_not_exist");
      Assert.fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(ERROR_CODE_DOMAIN_OBJECT_DOES_NOT_EXIST));
    }
    finally
    {
      connection.close();
    }
  }

  /**
   * Test case to make sure 2 non null bind refs was not constant folded into one
   */
  @Test
  public void testSnow36284() throws Exception
  {
    Connection connection = getConnection();

    String query = "select * from (values ('a'), ('b')) x where x.COLUMN1 in (?,?);";
    PreparedStatement preparedStatement = connection.prepareStatement(
        query);
    preparedStatement.setString(1, "a");
    preparedStatement.setString(2, "b");
    ResultSet rs = preparedStatement.executeQuery();
    int rowcount = 0;
    Set<String> valuesReturned = Sets.newHashSetWithExpectedSize(2);
    while (rs.next())
    {
      rowcount++;
      valuesReturned.add(rs.getString(1));
    }
    assertEquals("Should get back 2 rows", 2, rowcount);
    assertEquals("", valuesReturned, Sets.newHashSet("a", "b"));
  }

  /**
   * Test for coalesce with bind and null arguments in a prepared statement
   */
  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testSnow35923() throws Exception
  {
    Connection connection = getConnection();

    Statement statement = connection.createStatement();

    statement.execute("alter session set enable_false_filter_rule=false");
    statement.execute("alter session set "
                      + "optimizer_eliminate_scans_for_constant_select=false");
    statement.execute("create or replace table inc(a int, b int)");
    statement.execute("insert into inc(a, b) values (1, 2), "
                      + "(NULL, 4), (5,NULL), (7,8)");
    // Query used to cause an incident.
    String query = "SELECT coalesce(?, NULL) from inc;";
    PreparedStatement preparedStatement = connection.prepareStatement(
        query);
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();


    connection.close();
  }

  /**
   * Tests binding of object literals, including binding with object names as
   * well as binding with object IDs
   */
  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testBindObjectLiteral() throws Exception
  {
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();

    try
    {
      String sqlText = "create or replace table identifier(?) (c1 number)";
      SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      String t1 = "bindObjectTable1";
      // Bind the table name
      pStmt.setString(1, t1);
      ResultSet result = pStmt.executeQuery();

      // Verify the table has been created and get the table ID
      stmt.execute("select parse_json(system$dict_id('table', '" +
                   t1 + "')):entityId;");
      result = stmt.getResultSet();

      long t1Id = 0;
      if (result.next())
      {
        t1Id = Long.valueOf(result.getString(1));
      }

      assertTrue(t1Id != 0);

      // Mix of object literal binds and value binds
      sqlText = "insert into identifier(?) values (1), (2), (3)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      // Bind by object IDs
      pStmt.setLong(1, t1Id);

      result = pStmt.executeQuery();

      // Perform some selection
      sqlText = "select * from identifier(?) order by 1";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t1);
      result = pStmt.executeQuery();
      // Verify 3 rows have been inserted
      for (int i = 0; i < 3; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Alter Table
      sqlText = "alter table identifier(?) add column c2 number";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setLong(1, t1Id);
      result = pStmt.executeQuery();

      // Describe
      sqlText = "desc table identifier(?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t1);
      result = pStmt.executeQuery();
      // Verify two columns have been created
      for (int i = 0; i < 2; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Create another table
      String t2 = "bindObjectTable2";
      sqlText = "create or replace table identifier(?) (c1 number)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t2);
      result = pStmt.executeQuery();

      // Verify the table has been created and get the table ID
      stmt.execute("select parse_json(system$dict_id('table', '" +
                   t2 + "')):entityId;");
      result = stmt.getResultSet();

      long t2Id = 0;
      if (result.next())
      {
        t2Id = Long.valueOf(result.getString(1));
      }

      assertTrue(t2Id != 0);

      // Mix object binds with value binds
      sqlText = "insert into identifier(?) values (?), (?), (?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t2);
      pStmt.setInt(2, 1);
      pStmt.setInt(3, 2);
      pStmt.setInt(4, 3);
      result = pStmt.executeQuery();

      // Verify that 3 rows have been inserted
      sqlText = "select * from identifier(?) order by 1";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setLong(1, t2Id);
      result = pStmt.executeQuery();
      for (int i = 0; i < 3; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Multiple Object Binds
      sqlText = "select t2.c1 from identifier(?) as t1, identifier(?) as t2 " +
                "where t1.c1 = t2.c1 and t1.c1 > (?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setString(1, t1);
      pStmt.setLong(2, t2Id);
      pStmt.setInt(3, 1);
      result = pStmt.executeQuery();
      for (int i = 0; i < 2; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Drop Tables
      sqlText = "drop table identifier(?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, "bindObjectTable1");
      result = pStmt.executeQuery();

      sqlText = "drop table identifier(?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setLong(1, t2Id);
      result = pStmt.executeQuery();

      // Verify that the tables have been dropped
      stmt.execute("show tables like 'bindobjecttable%'");
      result = stmt.getResultSet();
      assertFalse(result.next());
    }
    catch (SQLException e)
    {
      // The test has failed if we encountered any exception
      assertTrue(false);
    }
    finally
    {
      conn.close();
    }
  }

  @Test
  public void testBindTimestampTZViaString() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set timestamp_tz_output_format='YYYY-MM" +
                      "-DD HH24:MI:SS.FF9 TZHTZM'");
    statement.execute("create or replace  table testbindtstz(cola timestamp_tz)");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into testbindtstz values(?)");

    preparedStatement.setString(1, "2017-11-30T18:17:05.123456789+08:00");
    int count = preparedStatement.executeUpdate();
    assertThat(count, is(1));

    resultSet = statement.executeQuery("select * from testbindtstz");
    assertTrue(resultSet.next());

    assertThat(resultSet.getString(1), is("2017-11-30 18:17:05.123456789 +0800"));

    resultSet.close();
    statement.execute("drop table if exists testbindtstz");

    statement.close();
    connection.close();
  }

  /**
   * Ensures binding a string type with TIMESTAMP_TZ works. The customer
   * has to use the specific timestamp format:
   * YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM
   */
  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testBindTimestampTZViaStringBatch() throws SQLException
  {
    Connection connection = getConnection();
    connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
    Statement statement = connection.createStatement();
    statement.execute("create or replace table testbindtstz(cola timestamp_tz, colb timestamp_ntz)");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into testbindtstz values(?,?)");

    preparedStatement.setString(1, "2017-11-30 18:17:05.123456789 +08:00");
    preparedStatement.setString(2, "2017-11-30 18:17:05.123456789");
    preparedStatement.addBatch();
    preparedStatement.setString(1, "2017-05-03 16:44:42.0");
    preparedStatement.setString(2, "2017-05-03 16:44:42.0");
    preparedStatement.addBatch();
    int[] count = preparedStatement.executeBatch();
    assertThat(count[0], is(1));

    resultSet = statement.executeQuery("select * from testbindtstz order by 1 desc");

    assertTrue(resultSet.next());
    assertThat(resultSet.getString(1), is("Thu, 30 Nov 2017 18:17:05 +0800"));
    assertThat(resultSet.getString(2), is("Thu, 30 Nov 2017 18:17:05 Z"));

    assertTrue(resultSet.next());
    assertThat(resultSet.getString(1), is("Wed, 03 May 2017 16:44:42 -0700"));
    assertThat(resultSet.getString(2), is("Wed, 03 May 2017 16:44:42 Z"));

    resultSet.close();
    statement.execute("drop table if exists testbindtstz");

    statement.close();
    connection.close();
  }

  /**
   * Test to ensure that when SqlBindRef is in a SqlConstantList
   * the sub-expression remover does not treat expressions with different
   * bind values are same expressions. SNOW-41620
   *
   * @throws Exception raises if any error occurs
   */
  @Test
  public void testSnow41620() throws Exception
  {
    connection = getConnection();
    statement = connection.createStatement();

    // Create a table and insert 3 records
    statement.execute("CREATE or REPLACE TABLE SNOW41620 (" +
                      "c1 varchar(20)," + "c2 int" + "  )");
    statement.execute("insert into SNOW41620 values('value1', 1), " +
                      "('value2', 2), ('value3', 3)");

    String PARAMETERIZED_QUERY = "SELECT t0.C1, " +
                                 "CASE WHEN t0.C1 IN (?, ?) THEN t0.C2  ELSE null END, " +
                                 "CASE WHEN t0.C1 IN (?, ?) THEN t0.C2  ELSE null END " +
                                 "FROM SNOW41620 t0";

    PreparedStatement pst = connection.prepareStatement(PARAMETERIZED_QUERY);
    // bind values
    pst.setObject(1, "value1");
    pst.setObject(2, "value3");
    pst.setObject(3, "value2");
    pst.setObject(4, "value3");
    ResultSet bindStmtResultSet = pst.executeQuery();

    // Execute the same query with bind values replaced in the sql
    String DIRECT_QUERY = "SELECT t0.C1, \n" +
                          "CASE WHEN t0.C1 IN ('value1', 'value3') THEN t0.C2  ELSE null END," +
                          "CASE WHEN t0.C1 IN ('value2', 'value3') THEN t0.C2  ELSE null END " +
                          "FROM SNOW41620 t0";

    pst = connection.prepareStatement(DIRECT_QUERY);
    ResultSet directStmtResultSet = pst.executeQuery();

    checkResultSetEqual(bindStmtResultSet, directStmtResultSet);

    bindStmtResultSet.close();
    directStmtResultSet.close();
    pst.close();
  }

  @Test
  public void testSnow50141() throws Exception
  {
    connection = getConnection();
    prepStatement = connection.prepareStatement("select 1 where true=?");
    prepStatement.setObject(1, true);
    resultSet = prepStatement.executeQuery();
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(1));
    assertThat(resultSet.next(), is(false));
    resultSet.close();
    connection.close();
  }

  @Test
  public void testPrepareUDTF() throws Exception
  {
    connection = getConnection();
    try
    {
      statement = connection.createStatement();
      statement.execute("create or replace table employee(id number, address text)");
      statement.execute("create or replace function employee_detail(sid number, addr text)\n" +
                        " returns table(id number, address text)\n" +
                        "LANGUAGE SQL\n" +
                        "as\n" +
                        "$$\n" +
                        "select *\n" +
                        "from employee\n" +
                        "where  id=sid\n" +
                        "$$;");

      // should resolve successfully
      prepStatement = connection.prepareStatement("select * from table(employee_detail(?, ?))");
      prepStatement.setInt(1, 1);
      prepStatement.setString(2, "abc");
      prepStatement.execute();

      //should resolve successfully
      prepStatement = connection.prepareStatement("select * from table(employee_detail(?, 'abc'))");
      prepStatement.setInt(1, 1);
      prepStatement.execute();

      try
      {
        // resolve fail because second argument is valid
        prepStatement = connection.prepareStatement("select * from table(employee_detail(?, 123))");
        Assert.fail();
      }
      catch (SQLException e)
      {
        // failed because argument type did not match
        Assert.assertThat(e.getErrorCode(), is(1044));
      }

      // create a udf with same name but different arguments and return type
      statement.execute("create or replace function employee_detail(name text , addr text)\n" +
                        " returns table(id number)\n" +
                        "LANGUAGE SQL\n" +
                        "as\n" +
                        "$$\n" +
                        "select id\n" +
                        "from employee\n" +
                        "$$;");
      try
      {
        // resolve fail because second argument is valid
        prepStatement = connection.prepareStatement("select * from table(employee_detail(?, 'abc'))");
        Assert.fail();
      }
      catch (SQLException e)
      {
        // failed because argument type did not match
        Assert.assertThat(e.getErrorCode(), is(2237));
      }
    }
    finally
    {
      statement.execute("drop function if exists employee_detail(number, text)");
      statement.execute("drop function if exists employee_detail(text, text)");

      connection.close();
    }
  }

  /**
   * Check if both the {@link ResultSet} passed in has equivalent data.
   * Checks the toString of the underlying data returned.
   *
   * @param rs1 ResultSet1 to compare
   * @param rs2 ResultSet2 to compare
   * @throws SQLException Will be thrown if any of the Driver calls fail
   */
  private void checkResultSetEqual(ResultSet rs1, ResultSet rs2)
  throws SQLException
  {
    int columns = rs1.getMetaData().getColumnCount();
    assertEquals(
        "Resultsets do not match in the number of columns returned",
        columns, rs2.getMetaData().getColumnCount());

    while (rs1.next() && rs2.next())
    {
      for (int columnIndex = 1; columnIndex <= columns; columnIndex++)
      {
        final Object res1 = rs1.getObject(columnIndex);
        final Object res2 = rs2.getObject(columnIndex);

        assertEquals(
            String.format("%s and %s are not equal values at column %d",
                          res1, res2, columnIndex),
            res1, res2);
      }

      assertEquals(
          "Number of records returned by the results does not match",
          rs1.isLast(), rs2.isLast());
    }
  }

  private void bindOneParamSet(PreparedStatement prepst,
                               int id,
                               double colA,
                               float colB,
                               String colC,
                               long colD,
                               short colE) throws SQLException
  {
    prepst.setInt(1, id);
    prepst.setDouble(2, colA);
    prepst.setFloat(3, colB);
    prepst.setString(4, colC);
    prepst.setLong(5, colD);
    prepst.setShort(6, colE);
  }

  @Test
  public void testPreparedStatementWithSkipParsing() throws Exception
  {
    Connection con = getConnection();
    PreparedStatement stmt = con.unwrap(SnowflakeConnectionV1.class).prepareStatement(
        "select 1", true);
    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next(), is(true));
    assertThat(rs.getInt(1), is(1));
    rs.close();
    con.close();
  }

  @Test
  public void testPreparedStatementWithSkipParsingAndBinding() throws Exception
  {
    Connection con = getConnection();
    con.createStatement().execute("create or replace table t(c1 int)");
    try
    {
      PreparedStatement stmt = con.unwrap(SnowflakeConnectionV1.class).prepareStatement(
          "insert into t(c1) values(?)", true);
      stmt.setInt(1, 123);
      int ret = stmt.executeUpdate();
      assertThat(ret, is(1));
      stmt = con.unwrap(SnowflakeConnectionV1.class).prepareStatement(
          "select * from t", true);
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next(), is(true));
      assertThat(rs.getInt(1), is(123));
      rs.close();
    }
    finally
    {
      con.createStatement().execute("drop table if exists t");
      con.close();
    }
  }

  /**
   * Test prepare mode for to_timestamp(?, 3), compiler right now can not
   * handle this properly. A workaround is added.
   * More specifically, ErrorCode returned for this statement is caught in
   * SnowflakePreparedStatementV1 so that execution can continue
   */
  @Test
  public void testSnow44393() throws Exception
  {
    Connection con = getConnection();
    con.createStatement().execute("alter session set timestamp_ntz_output_format='YYYY-MM-DD HH24:MI:SS'");
    try
    {
      PreparedStatement stmt = con.prepareStatement("select to_timestamp_ntz(?, 3)");
      stmt.setBigDecimal(1, new BigDecimal("1261440000000"));
      ResultSet resultSet = stmt.executeQuery();
      resultSet.next();

      String res = resultSet.getString(1);
      assertThat(res, is("2009-12-22 00:00:00"));
    }
    finally
    {
      con.close();
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testAddBatchNumericNullFloatMixed() throws Exception
  {
    Connection connection = getConnection();
    PreparedStatement stmt = null;
    try
    {
      for (int threshold = 0; threshold < 2; ++threshold)
      {
        connection.createStatement().execute(
            "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = " +
            threshold); // enable stage bind
        String sql = "insert into TEST_PREPST(col, colB) values(?,?)";

        // Null NUMERIC followed by FLOAT.
        stmt = connection.prepareStatement(sql);
        stmt.setInt(1, 1);
        stmt.setNull(2, java.sql.Types.NUMERIC);
        stmt.addBatch();
        stmt.setInt(1, 2);
        stmt.setObject(2, 4.0f);
        stmt.addBatch();
        stmt.executeBatch();

        // evaluate query ids
        assertTrue(stmt.isWrapperFor(SnowflakePreparedStatement.class));
        String qid1 = stmt.unwrap(SnowflakePreparedStatement.class).getQueryID();
        assertNotNull(qid1);

        // Null CHAR followed by FLOAT. CHAR type is ignored.
        stmt = connection.prepareStatement(sql);
        stmt.setInt(1, 1);
        stmt.setNull(2, java.sql.Types.CHAR);
        stmt.addBatch();
        stmt.setInt(1, 2);
        stmt.setObject(2, 4.0f);
        stmt.addBatch();
        stmt.executeBatch();

        // FLOAT followed by Null NUMERIC. Type for null value is ignored
        // anyway.
        stmt = connection.prepareStatement(sql);
        stmt.setInt(1, 2);
        stmt.setObject(2, 4.0f);
        stmt.addBatch();
        stmt.setInt(1, 1);
        stmt.setNull(2, java.sql.Types.NUMERIC);
        stmt.addBatch();
        stmt.setInt(1, 1);
        stmt.setNull(2, java.sql.Types.BOOLEAN);
        stmt.addBatch();
        stmt.executeBatch();

        // Null NUMERIC followed by FLOAT, but STRING won't be allowed.
        stmt = connection.prepareStatement(sql);
        stmt.setInt(1, 1);
        stmt.setNull(2, java.sql.Types.NUMERIC);
        stmt.addBatch();
        stmt.setInt(1, 2);
        stmt.setObject(2, 4.0f);
        stmt.addBatch();
        stmt.setInt(1, 1);
        stmt.setObject(2, "test1");
        try
        {
          stmt.addBatch();
          fail("Must fail");
        }
        catch (SnowflakeSQLException ex)
        {
          assertThat("Error code is wrong",
                     ex.getErrorCode(),
                     equalTo(ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode()));
          assertThat("Location",
                     ex.getMessage(), containsString("Column: 2, Row: 3"));
        }
      }
    }
    finally
    {
      if (stmt != null)
      {
        stmt.close();
      }
      connection.close();
    }
  }

  @Test
  public void testInvalidUsageOfApi() throws Exception
  {
    Connection connection = getConnection();
    final PreparedStatement preparedStatement = connection.prepareStatement(
        "select 1");
    final int expectedCode =
        ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API.getMessageCode();

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.executeUpdate("select 1");
      }
    }, expectedCode);

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.executeQuery("select 1");
      }
    }, expectedCode);

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.execute("select 1");
      }
    }, expectedCode);

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.addBatch("select 1");
      }
    }, expectedCode);

  }

  private void assertException(RunnableWithSQLException runnable,
                               int expectedCode)
  {
    try
    {
      runnable.run();
      Assert.fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(expectedCode));
    }
  }

  private interface RunnableWithSQLException
  {
    void run() throws SQLException;
  }

  @Test
  public void testCreatePreparedStatementWithParameters() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      try
      {
        connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        fail("updateable cursor is not supported.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }
      connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                  ResultSet.CLOSE_CURSORS_AT_COMMIT);
      try
      {
        connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
        fail("hold cursor over commit is not supported.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }

    }
  }

  @Test
  public void testFeatureNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      PreparedStatement preparedStatement = connection.prepareStatement("select ?");
      expectFeatureNotSupportedException(() -> preparedStatement.setArray(1, new FakeArray()));
      expectFeatureNotSupportedException(() -> preparedStatement.setAsciiStream(1, new FakeInputStream()));
      expectFeatureNotSupportedException(() -> preparedStatement.setAsciiStream(1, new FakeInputStream(), 1));
      expectFeatureNotSupportedException(() -> preparedStatement.setBinaryStream(1, new FakeInputStream()));
      expectFeatureNotSupportedException(() -> preparedStatement.setBinaryStream(1, new FakeInputStream(), 1));
      expectFeatureNotSupportedException(() -> preparedStatement.setCharacterStream(1, new FakeReader()));
      expectFeatureNotSupportedException(() -> preparedStatement.setCharacterStream(1, new FakeReader(), 1));
      expectFeatureNotSupportedException(() -> preparedStatement.setRef(1, new FakeRef()));
      expectFeatureNotSupportedException(() -> preparedStatement.setBlob(1, new FakeBlob()));

      URL fakeURL = new URL("http://localhost:8888/");
      expectFeatureNotSupportedException(() -> preparedStatement.setURL(1, fakeURL));

      expectFeatureNotSupportedException(() -> preparedStatement.setRowId(1, new FakeRowId()));
      expectFeatureNotSupportedException(() -> preparedStatement.setNString(1, "test"));
      expectFeatureNotSupportedException(() -> preparedStatement.setNCharacterStream(1, new FakeReader()));
      expectFeatureNotSupportedException(() -> preparedStatement.setNCharacterStream(1, new FakeReader(), 1));
      expectFeatureNotSupportedException(() -> preparedStatement.setNClob(1, new FakeNClob()));
      expectFeatureNotSupportedException(() -> preparedStatement.setNClob(1, new FakeReader(), 1));

      expectFeatureNotSupportedException(() -> preparedStatement.setClob(1, new FakeReader()));
      expectFeatureNotSupportedException(() -> preparedStatement.setClob(1, new FakeReader(), 1));
      expectFeatureNotSupportedException(() -> preparedStatement.setBlob(1, new FakeInputStream()));
      expectFeatureNotSupportedException(() -> preparedStatement.setBlob(1, new FakeInputStream(), 1));
      expectFeatureNotSupportedException(() -> preparedStatement.setSQLXML(1, new FakeSQLXML()));

      expectFeatureNotSupportedException(() -> preparedStatement.execute("select 1", 1));
      expectFeatureNotSupportedException(() -> preparedStatement.execute("select 1", new int[]{}));
      expectFeatureNotSupportedException(() -> preparedStatement.execute("select 1", new String[]{}));
      expectFeatureNotSupportedException(() -> preparedStatement.executeUpdate("select 1", 1));
      expectFeatureNotSupportedException(() -> preparedStatement.executeUpdate("select 1", new int[]{}));
      expectFeatureNotSupportedException(() -> preparedStatement.executeUpdate("select 1", new String[]{}));
    }
  }
}

