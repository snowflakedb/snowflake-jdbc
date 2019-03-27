/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.loader;

import net.snowflake.client.AbstractDriverIT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * Loader IT
 */
public class LoaderIT
{
  private final static String TARGET_TABLE_NAME = "LOADER_test_TABLE";

  private static Connection testConnection;
  private static Connection putConnection;
  private static String SCHEMA_NAME;

  @BeforeClass
  public static void setUpClass() throws Throwable
  {
    testConnection = AbstractDriverIT.getConnection();
    putConnection = AbstractDriverIT.getConnection();

    SCHEMA_NAME = testConnection.getSchema();
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE \"%s\" ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 varchar(255) DEFAULT 'X', "
        + "C3 double, "
        + "C4 timestamp, "
        + "C5 variant)",
        LoaderIT.TARGET_TABLE_NAME));
  }

  @AfterClass
  public static void tearDownClass() throws SQLException
  {
    testConnection.createStatement().execute(
        String.format("DROP TABLE IF EXISTS \"%s\"", LoaderIT.TARGET_TABLE_NAME));

    testConnection.close();
    putConnection.close();
  }

  @Test
  public void testInjectBadStagedFileInsert() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    StreamLoader loader = tdcb.setOnError("ABORT_STATEMENT").getStreamLoader();
    listener.throwOnError = true;
    int numberOfRows = 1000;
    loader.setProperty(LoaderProperty.testRemoteBadCSV, true);
    loader.setProperty(LoaderProperty.startTransaction, true);
    loader.start();
    Random rnd = new Random();

    // generates a new data set and ingest
    for (int i = 0; i < numberOfRows; i++)
    {
      final String json = "{\"key\":" + rnd.nextInt() + ","
                          + "\"bar\":" + i + "}";
      Object[] row = new Object[]
          {
              i, "foo_" + i, rnd.nextInt() / 3, new Date(),
              json
          };
      loader.submitRow(row);
    }
    try
    {
      loader.finish();
      fail("Should raise and error");
    }
    catch (Loader.DataError ex)
    {
      assertThat("Loader.DataError is raised", true);
    }
  }

  @Test
  public void testExecuteBeforeAfterSQLError() throws Exception
  {
    TestDataConfigBuilder tdcbBefore = new TestDataConfigBuilder(
        testConnection, putConnection);
    StreamLoader loaderBefore = tdcbBefore.setOnError("ABORT_STATEMENT").getStreamLoader();
    loaderBefore.setProperty(
        LoaderProperty.executeBefore, "SELECT * FROOOOOM TBL");
    loaderBefore.start();
    try
    {
      loaderBefore.finish();
      fail("SQL Error should be raised.");
    }
    catch (Loader.ConnectionError e)
    {
      assertThat(e.getCause(), instanceOf(SQLException.class));
    }

    TestDataConfigBuilder tdcbAfter = new TestDataConfigBuilder(
        testConnection, putConnection);
    StreamLoader loaderAfter = tdcbAfter.setOnError("ABORT_STATEMENT").getStreamLoader();
    loaderAfter.setProperty(
        LoaderProperty.executeBefore, "select current_version()");
    loaderAfter.setProperty(
        LoaderProperty.executeAfter, "SELECT * FROM TBBBBBBL");
    loaderAfter.start();
    try
    {
      loaderAfter.finish();
      fail("SQL Error should be raised.");
    }
    catch (Loader.ConnectionError e)
    {
      assertThat(e.getCause(), instanceOf(SQLException.class));
    }
  }

  /**
   * This is to run performance tests. Set the environment variables
   * and run this by VisualVM, YourKit, or any profiler.
   * <p>
   * SNOWFLAKE_TEST_ACCOUNT=testaccount
   * SNOWFLAKE_TEST_USER=testuser
   * SNOWFLAKE_TEST_PASSWORD=testpassword
   * SNOWFLAKE_TEST_HOST=testaccount.snowflakecomputing.com
   * SNOWFLAKE_TEST_PORT=443
   * SNOWFLAKE_TEST_DATABASE=testdb
   * SNOWFLAKE_TEST_SCHEMA=public
   * SNOWFLAKE_TEST_WAREHOUSE=testwh
   * SNOWFLAKE_TEST_ROLE=sysadmin
   * SNOWFLAKE_TEST_PROTOCOL=https
   *
   * @throws Exception raises an exception if any error occurs.
   */
  @Ignore("Performance test")
  @Test
  public void testLoaderLargeInsert() throws Exception
  {
    new TestDataConfigBuilder(testConnection, putConnection)
        .setDatabaseName("INFORMATICA_DB")
        .setCompressDataBeforePut(false)
        .setCompressFileByPut(true)
        .setNumberOfRows(10000000)
        .setCsvFileSize(100000000L)
        .setCsvFileBucketSize(64)
        .populate();
  }

  @Test
  public void testLoaderInsert() throws Exception
  {
    // mostly just populate test data but with delay injection to test
    // PUT retry
    new TestDataConfigBuilder(testConnection, putConnection)
        .setTestMode(true).populate();
  }

  @Test
  public void testLoaderMultipleBatch() throws Exception
  {
    String refTableName = "LOADER_TEST_TABLE_REF";
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE \"%s\" ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 varchar(255) DEFAULT 'X', "
        + "C3 double, "
        + "C4 timestamp, "
        + "C5 variant)", refTableName));

    try
    {
      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
          testConnection, putConnection);
      List<Object[]> dataSet = tdcb.populateReturnData();

      TestDataConfigBuilder tdcbRef = new TestDataConfigBuilder(
          testConnection, putConnection);
      tdcbRef.setDataSet(dataSet)
          .setTableName(refTableName)
          .setCsvFileBucketSize(2)
          .setCsvFileSize(30000).populate();

      ResultSet rsReference = testConnection.createStatement().executeQuery(String.format(
          "SELECT hash_agg(*) FROM \"%s\"", TARGET_TABLE_NAME
      ));
      rsReference.next();
      long hashValueReference = rsReference.getLong(1);
      ResultSet rsTarget = testConnection.createStatement().executeQuery(String.format(
          "SELECT hash_agg(*) FROM \"%s\"", refTableName
      ));
      rsTarget.next();
      long hashValueTarget = rsTarget.getLong(1);
      assertThat("hash values", hashValueTarget, equalTo(hashValueReference));
    }
    finally
    {
      testConnection.createStatement().execute(String.format(
          "DROP TABLE IF EXISTS %s", refTableName));
    }
  }

  @Test
  public void testLoadTime() throws Exception
  {
    String tableName = "LOADER_TIME_TEST";
    try
    {
      testConnection.createStatement().execute(String.format(
          "CREATE OR REPLACE TABLE %s ("
          + "ID int, "
          + "C1 time, C2 date)", tableName));

      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
          testConnection, putConnection);
      tdcb
          .setTableName(tableName)
          .setStartTransaction(true)
          .setTruncateTable(true)
          .setColumns(Arrays.asList(
              "ID", "C1", "C2"))
          .setKeys(Collections.singletonList(
              "ID"
          ));
      StreamLoader loader = tdcb.getStreamLoader();
      TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
      loader.start();
      Time tm = new Time(3723000);
      Date dt = new Date();
      for (int i = 0; i < 10; i++)
      {
        Object[] row = new Object[]{i, tm, dt};
        loader.submitRow(row);
      }
      loader.finish();
      String errorMessage = "";
      if (listener.getErrorCount() > 0)
      {
        errorMessage = listener.getErrors().get(0).getException().toString();
      }
      assertThat(
          String.format("Error: %s", errorMessage),
          listener.getErrorCount(), equalTo(0));
      ResultSet rs = testConnection.createStatement().executeQuery(
          String.format(
              "SELECT c1, c2 FROM %s LIMIT 1", tableName));
      rs.next();
      Time rsTm = rs.getTime(1);
      Date rsDt = rs.getDate(2);
      assertThat("Time column didn't match", rsTm, equalTo(tm));

      Calendar cal = cutOffTimeFromDate(dt);
      long dtEpoch = cal.getTimeInMillis();
      long rsDtEpoch = rsDt.getTime();
      assertThat("Date column didn't match", rsDtEpoch, equalTo(dtEpoch));
    }
    finally
    {
      testConnection.createStatement().execute(String.format(
          "DROP TABLE IF EXISTS %s", tableName));
    }
  }

  private Calendar cutOffTimeFromDate(Date dt)
  {
    Calendar cal = Calendar.getInstance(); // locale-specific
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    cal.setTime(dt);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal;
  }

  @Test
  public void testLoaderDelete() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbDelete = new TestDataConfigBuilder(
        testConnection, putConnection
    );
    tdcbDelete
        .setOperation(Operation.DELETE)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1"
        ))
        .setKeys(Arrays.asList(
            "ID", "C1"
        ));
    StreamLoader loader = tdcbDelete.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbDelete.getListener();
    loader.start();

    Object[] del = new Object[]
        {
            42, "foo_42" // deleted
        };
    loader.submitRow(del);

    del = new Object[]
        {
            41, "blah" // ignored, and should not raise any error/warning
        };
    loader.submitRow(del);
    loader.finish();

    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count",
               listener.getErrorRecordCount(), equalTo(0));
    assertThat("submitted row count",
               listener.getSubmittedRowCount(), equalTo(2));
    assertThat("processed", listener.processed.get(), equalTo(1));
    assertThat("deleted rows", listener.deleted.get(), equalTo(1));
  }

  @Test
  public void testLoaderModify() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbModify = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbModify
        .setOperation(Operation.MODIFY)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbModify.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbModify.getListener();
    loader.start();

    Object[] mod = new Object[]
        {
            41, "modified", "some\nthi\"ng\\", 41.6, new Date(), "{}"
        };
    loader.submitRow(mod);
    mod = new Object[]
        {
            40, "modified", "\"something,", 40.2, new Date(), "{}"
        };
    loader.submitRow(mod);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    // Test deletion
    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N"
                      + " FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("count is not correct", rs.getInt("N"), equalTo(10000));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1 AS N"
                      + " FROM \"%s\" WHERE ID=40", TARGET_TABLE_NAME));

    rs.next();
    assertThat("status is not correct", rs.getString("N"), equalTo("modified"));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1, C2"
                      + " FROM \"%s\" WHERE ID=41", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C1 is not correct",
               rs.getString("C1"), equalTo("modified"));
    assertThat("C2 is not correct",
               rs.getString("C2"), equalTo("some\nthi\"ng\\"));
  }

  @Test
  public void testLoaderModifyWithOneMatchOneNot() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbModify = new TestDataConfigBuilder(
        testConnection, putConnection
    );
    tdcbModify
        .setTruncateTable(false)
        .setOperation(Operation.MODIFY)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbModify.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbModify.getListener();
    loader.start();

    Object[] mod = new Object[]
        {
            20000, "modified", "some\nthi\"ng\\", 41.6, new Date(), "{}"
        };
    loader.submitRow(mod);
    mod = new Object[]
        {
            45, "modified", "\"something2,", 40.2, new Date(), "{}"
        };
    loader.submitRow(mod);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(1));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated", listener.updated.get(), equalTo(1));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    // Test deletion
    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N"
                      + " FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("count is not correct", rs.getInt("N"), equalTo(10000));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1, C2"
                      + " FROM \"%s\" WHERE ID=45", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C1 is not correct",
               rs.getString("C1"), equalTo("modified"));
    assertThat("C2 is not correct",
               rs.getString("C2"), equalTo("\"something2,"));
  }

  @Test
  public void testLoaderUpsert() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    loader.start();

    Date d = new Date();

    Object[] ups = new Object[]
        {
            10001, "inserted\\,", "something", 0x4.11_33p2, d, "{}"
        };
    loader.submitRow(ups);
    ups = new Object[]
        {
            39, "modified", "something", 40.1, d, "{}"
        };
    loader.submitRow(ups);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated/inserted", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1, C4, C3"
                      + " FROM \"%s\" WHERE ID=10001", TARGET_TABLE_NAME));

    rs.next();
    assertThat("C1 is not correct", rs.getString("C1"), equalTo("inserted\\,"));

    long l = rs.getTimestamp("C4").getTime();
    assertThat("C4 is not correct", l, equalTo(d.getTime()));
    assertThat("C3 is not correct", Double.toHexString((rs.getDouble("C3"))),
               equalTo("0x1.044cc0000225cp4"));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1 AS N"
                      + " FROM \"%s\" WHERE ID=39", TARGET_TABLE_NAME));

    rs.next();
    assertThat("N is not correct", rs.getString("N"), equalTo("modified"));
  }

  @Test
  public void testLoaderUpsertWithError() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    loader.start();

    Object[] upse = new Object[]
        {
            "10001-", "inserted", "something", "42-", new Date(), "{}"
        };
    loader.submitRow(upse);
    upse = new Object[]
        {
            10002, "inserted", "something", 43, new Date(), "{}"
        };
    loader.submitRow(upse);
    upse = new Object[]
        {
            45, "modified", "something", 46.1, new Date(), "{}"
        };
    loader.submitRow(upse);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(3));
    assertThat("counter", listener.counter.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(3));
    assertThat("updated/inserted", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(2));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(1));
    assertThat("Target table name is not correct", listener.getErrors().get(0)
        .getTarget(), equalTo(TARGET_TABLE_NAME));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N"
                      + " FROM \"%s\"", TARGET_TABLE_NAME));

    rs.next();
    int c = rs.getInt("N");
    assertThat("N is not correct", c, equalTo(10001));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1 AS N"
                      + " FROM \"%s\" WHERE ID=45", TARGET_TABLE_NAME));

    rs.next();
    assertThat("N is not correct", rs.getString("N"), equalTo("modified"));
  }

  @Test
  public void testLoaderUpsertWithErrorAndRollback() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    PreparedStatement pstmt = testConnection.prepareStatement(
        String.format("INSERT INTO \"%s\"(ID,C1,C2,C3,C4,C5)"
                      + " SELECT column1, column2, column3, column4,"
                      + " column5, parse_json(column6)"
                      + " FROM VALUES(?,?,?,?,?,?)", TARGET_TABLE_NAME));
    pstmt.setInt(1, 10001);
    pstmt.setString(2, "inserted\\,");
    pstmt.setString(3, "something");
    pstmt.setDouble(4, 0x4.11_33p2);
    pstmt.setDate(5, new java.sql.Date(new Date().getTime()));
    pstmt.setObject(6, "{}");
    pstmt.execute();
    testConnection.commit();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setStartTransaction(true)
        .setPreserveStageFile(true)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    listener.throwOnError = true; // should trigger rollback
    loader.start();
    try
    {

      Object[] noerr = new Object[]
          {
              "10001", "inserted", "something", "42", new Date(), "{}"
          };
      loader.submitRow(noerr);

      Object[] err = new Object[]
          {
              "10002-", "inserted", "something", "42-", new Date(), "{}"
          };
      loader.submitRow(err);

      loader.finish();

      fail("Test must raise Loader.DataError exception");
    }
    catch (Loader.DataError e)
    {
      // we are good
      assertThat("error message",
                 e.getMessage(), allOf(
              containsString("10002-"),
              containsString("not recognized")));
    }

    assertThat("processed", listener.processed.get(), equalTo(0));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated/inserted", listener.updated.get(), equalTo(0));
    assertThat("error count", listener.getErrorCount(), equalTo(2));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(1));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("N", rs.getInt("N"), equalTo(10001));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C3 FROM \"%s\" WHERE id=10001", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C3. No commit should happen",
               Double.toHexString((rs.getDouble("C3"))),
               equalTo("0x1.044cc0000225cp4"));
  }

  @Test
  public void testLoadTimestampV1() throws Exception
  {
    final String targetTableName = "LOADER_TEST_TIMESTAMP_V1";

    // create table including TIMESTAMP_NTZ
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE %s ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 timestamp_ntz)", targetTableName));

    // Binding java.sql.Time with TIMESTAMP is supported only if
    // mapTimeToTimestamp flag is enabled. This is required to keep the
    // old behavior of Informatica V1 connector.
    Object[] testData = new Object[]{
        // full timestamp in Time object. Interestingly all values are
        // preserved.
        new java.sql.Time(1502931205000L),
        java.sql.Time.valueOf("12:34:56") // a basic test case
    };

    for (int i = 0; i < 2; ++i)
    {
      boolean useLocalTimezone;
      TimeZone originalTimeZone;
      TimeZone targetTimeZone;

      if (i == 0)
      {
        useLocalTimezone = true;
        originalTimeZone = TimeZone.getDefault();
        targetTimeZone = TimeZone.getTimeZone("America/Los_Angeles");
      }
      else
      {
        useLocalTimezone = false;
        originalTimeZone = TimeZone.getTimeZone("UTC");
        targetTimeZone = TimeZone.getTimeZone("UTC");
      }

      // input timestamp associated with the target timezone, America/Los_Angeles
      for (Object testTs : testData)
      {
        _testLoadTimestamp(targetTableName, originalTimeZone,
                           targetTimeZone, testTs, useLocalTimezone, true);
      }
    }
  }

  @Test
  public void testLoadTimestamp() throws Exception
  {
    final String targetTableName = "LOADER_TEST_TIMESTAMP";

    // create table including TIMESTAMP_NTZ
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE %s ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 timestamp_ntz)", targetTableName));

    // Binding java.util.Date, Timestamp and java.sql.Date with TIMESTAMP
    // datatype. No java.sql.Time binding is supported for TIMESTAMP.
    // For java.sql.Time, the target data type must be TIME.
    Object[] testData = new Object[]{
        new Date(),
        java.sql.Timestamp.valueOf("0001-01-01 08:00:00"),
        java.sql.Date.valueOf("2001-01-02")
    };

    for (int i = 0; i < 2; ++i)
    {
      boolean useLocalTimezone = false;
      TimeZone originalTimeZone;
      TimeZone targetTimeZone;

      if (i == 0)
      {
        useLocalTimezone = true;
        originalTimeZone = TimeZone.getDefault();
        targetTimeZone = TimeZone.getTimeZone("America/Los_Angeles");
      }
      else
      {
        originalTimeZone = TimeZone.getTimeZone("UTC");
        targetTimeZone = TimeZone.getTimeZone("UTC");
      }

      // input timestamp associated with the target timezone, America/Los_Angeles
      for (Object testTs : testData)
      {
        _testLoadTimestamp(targetTableName, originalTimeZone,
                           targetTimeZone, testTs, useLocalTimezone, false);
      }
    }
  }

  private void _testLoadTimestamp(
      String targetTableName,
      TimeZone originalTimeZone, TimeZone targetTimeZone,
      Object testTs, boolean useLocalTimeZone,
      boolean mapTimeToTimestamp) throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);

    tdcb
        .setStartTransaction(true)
        .setTruncateTable(true)
        .setTableName(targetTableName)
        .setUseLocalTimezone(useLocalTimeZone)
        .setMapTimeToTimestamp(mapTimeToTimestamp)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2"
        ));
    StreamLoader loader = tdcb.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();

    TimeZone.setDefault(targetTimeZone); // change default timezone before start

    loader.start();

    for (int i = 0; i < 5; ++i)
    {
      Object[] row = new Object[]{
          i, "foo_" + i, testTs
      };
      loader.submitRow(row);
    }
    loader.finish();
    TimeZone.setDefault(originalTimeZone);

    assertThat("Loader detected errors",
               listener.getErrorCount(), equalTo(0));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT * FROM \"%s\"", targetTableName));

    rs.next();
    Timestamp ts = rs.getTimestamp("C2");

    // format the input TS with the target timezone
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    sdf.setTimeZone(targetTimeZone);
    String currenTsStr = sdf.format(testTs);

    // format the retrieved TS with the original timezone
    sdf.setTimeZone(originalTimeZone);
    String retrievedTsStr = sdf.format(new Date(ts.getTime()));

    // They must be identical.
    assertThat("Input and retrieved timestamp are different",
               retrievedTsStr, equalTo(currenTsStr));
  }

  @Test
  public void testEmptyFieldAsEmpty() throws Exception
  {
    _testEmptyFieldAsEmpty(true);
    _testEmptyFieldAsEmpty(false);
  }

  private void _testEmptyFieldAsEmpty(boolean copyEmptyFieldAsEmpty) throws Exception
  {
    String tableName = "LOADER_EMPTY_FIELD_AS_NULL";
    try
    {
      testConnection.createStatement().execute(String.format(
          "CREATE OR REPLACE TABLE %s ("
          + "ID int, "
          + "C1 string, C2 string)", tableName));

      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
          testConnection, putConnection
      );
      tdcb
          .setOperation(Operation.INSERT)
          .setStartTransaction(true)
          .setTruncateTable(true)
          .setTableName(tableName)
          .setCopyEmptyFieldAsEmpty(copyEmptyFieldAsEmpty)
          .setColumns(Arrays.asList(
              "ID", "C1", "C2"
          ));
      StreamLoader loader = tdcb.getStreamLoader();
      TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
      loader.start();

      // insert null
      loader.submitRow(new Object[]
                           {
                               1, null, ""
                           });
      loader.finish();
      int submitted = listener.getSubmittedRowCount();
      assertThat("submitted rows", submitted, equalTo(1));

      ResultSet rs = testConnection.createStatement().executeQuery(
          String.format("SELECT C1, C2 FROM %s", tableName));

      rs.next();
      String c1 = rs.getString(1); // null
      String c2 = rs.getString(2); // empty
      if (!copyEmptyFieldAsEmpty)
      {
        // COPY ... EMPTY_FIELD_AS_NULL = TRUE /* default */
        assertThat(c1, is(nullValue()));  // null => null
        assertThat(c2, equalTo("")); // empty => empty
      }
      else
      {
        // COPY EMPTY_FIELD_AS_NULL = FALSE
        assertThat(c1, equalTo("")); // null => empty
        assertThat(c2, equalTo("")); // empty => empty
      }
      rs.close();
    }
    finally
    {
      testConnection.createStatement().execute(String.format(
          "DROP TABLE IF EXISTS %s", tableName));
    }
  }

  /**
   * Test a target table name including spaces.
   *
   * @throws Exception raises if any error occurs
   */
  @Test
  public void testSpacesInColumnTable() throws Exception
  {
    String targetTableName = "Load Test Spaces In Columns";

    // create table with spaces in column names
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE \"%s\" ("
        + "ID int, "
        + "\"Column 1\" varchar(255))", targetTableName));

    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb
        .setTableName(targetTableName)
        .setColumns(Arrays.asList(
            "ID", "Column 1"
        ));

    StreamLoader loader = tdcb.getStreamLoader();
    loader.start();

    for (int i = 0; i < 5; ++i)
    {
      Object[] row = new Object[]{
          i, "foo_" + i
      };
      loader.submitRow(row);
    }
    loader.finish();

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT * FROM \"%s\" ORDER BY \"Column 1\"",
                      targetTableName));

    rs.next();
    assertThat("The first id", rs.getInt(1), equalTo(0));
    assertThat("The first str", rs.getString(2), equalTo("foo_0"));
  }

  @Test
  public void testLoaderInsertAbortStatement() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    StreamLoader loader = tdcb.setOnError("ABORT_STATEMENT").getStreamLoader();
    listener.throwOnError = true;

    loader.start();
    Random rnd = new Random();

    // generates a new data set and ingest
    for (int i = 0; i < 10; i++)
    {
      final String json = "{\"key\":" + String.valueOf(rnd.nextInt()) + ","
                          + "\"bar\":" + i + "}";
      Object v = rnd.nextInt() / 3;
      if (i == 7)
      {
        v = "INVALID_INTEGER";
      }
      Object[] row = new Object[]
          {
              i, "foo_" + i, v, new Date(), json
          };
      loader.submitRow(row);
    }
    try
    {
      loader.finish();
      fail("should raise an exception");
    }
    catch (Loader.DataError ex)
    {
      assertThat(ex.toString(), containsString("INVALID_INTEGER"));
    }
  }

  @Test
  public void testLoadTimestampMilliseconds() throws Exception
  {
    String srcTable = "LOAD_TIMESTAMP_MS_SRC";
    String dstTable = "LOAD_TIMESTAMP_MS_DST";

    testConnection.createStatement().execute(
        String.format("create or replace table %s(c1 int, c2 timestamp_ntz(9))", srcTable));
    testConnection.createStatement().execute(
        String.format("create or replace table %s like %s", dstTable, srcTable));
    testConnection.createStatement().execute(
        String.format("insert into %s(c1,c2) values" +
                      "(1, '2018-05-12 12:34:56.123456789')," +
                      "(2, '2018-05-13 03:45:27.988')," +
                      "(3, '2018-05-14 07:12:34')," +
                      "(4, '2018-12-15 10:43:45.000000876')",
                      srcTable));

    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    StreamLoader loader =
        tdcb.setOnError("ABORT_STATEMENT")
            .setSchemaName(SCHEMA_NAME)
            .setTableName(dstTable)
            .setPreserveStageFile(true)
            .setColumns(Arrays.asList(
                "C1", "C2"
            ))
            .getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    listener.throwOnError = true;

    loader.start();

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("select * from %s", srcTable));
    while (rs.next())
    {
      Object c1 = rs.getObject(1);
      Object c2 = rs.getObject(2);
      Object[] row = new Object[]
          {
              c1, c2
          };
      loader.submitRow(row);
    }
    loader.finish();

    rs = testConnection.createStatement().executeQuery(
        String.format("select * from %s minus select * from %s", dstTable, srcTable)
    );
    assertThat("No result", !rs.next());
  }
}
