package net.snowflake.client.loader;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Loader IT */
@Tag(TestTags.LOADER)
public class LoaderIT extends LoaderBase {
  @Test
  public void testInjectBadStagedFileInsert() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    StreamLoader loader = tdcb.setOnError("ABORT_STATEMENT").getStreamLoader();
    listener.throwOnError = true;
    int numberOfRows = 1000;
    loader.setProperty(LoaderProperty.testRemoteBadCSV, true);
    loader.setProperty(LoaderProperty.startTransaction, true);
    loader.start();
    Random rnd = new Random();

    // generates a new data set and ingest
    for (int i = 0; i < numberOfRows; i++) {
      final String json = "{\"key\":" + rnd.nextInt() + "," + "\"bar\":" + i + "}";
      Object[] row = new Object[] {i, "foo_" + i, rnd.nextInt() / 3, new Date(), json};
      loader.submitRow(row);
    }
    assertThrows(Loader.DataError.class, loader::finish);
  }

  @Test
  public void testExecuteBeforeAfterSQLError() throws Exception {
    TestDataConfigBuilder tdcbBefore = new TestDataConfigBuilder(testConnection, putConnection);
    StreamLoader loaderBefore = tdcbBefore.setOnError("ABORT_STATEMENT").getStreamLoader();
    loaderBefore.setProperty(LoaderProperty.executeBefore, "SELECT * FROOOOOM TBL");
    loaderBefore.start();
    Loader.ConnectionError e = assertThrows(Loader.ConnectionError.class, loaderBefore::finish);
    assertThat(e.getCause(), instanceOf(SQLException.class));

    TestDataConfigBuilder tdcbAfter = new TestDataConfigBuilder(testConnection, putConnection);
    StreamLoader loaderAfter = tdcbAfter.setOnError("ABORT_STATEMENT").getStreamLoader();
    loaderAfter.setProperty(LoaderProperty.executeBefore, "select current_version()");
    loaderAfter.setProperty(LoaderProperty.executeAfter, "SELECT * FROM TBBBBBBL");
    loaderAfter.start();
    e = assertThrows(Loader.ConnectionError.class, loaderAfter::finish);
    assertThat(e.getCause(), instanceOf(SQLException.class));
  }

  /**
   * This is to run performance tests. Set the environment variables and run this by VisualVM,
   * YourKit, or any profiler.
   *
   * <p>SNOWFLAKE_TEST_ACCOUNT=testaccount SNOWFLAKE_TEST_USER=testuser
   * SNOWFLAKE_TEST_PASSWORD=testpassword SNOWFLAKE_TEST_HOST=testaccount.snowflakecomputing.com
   * SNOWFLAKE_TEST_PORT=443 SNOWFLAKE_TEST_DATABASE=testdb SNOWFLAKE_TEST_SCHEMA=public
   * SNOWFLAKE_TEST_WAREHOUSE=testwh SNOWFLAKE_TEST_ROLE=sysadmin SNOWFLAKE_TEST_PROTOCOL=https
   *
   * @throws Exception raises an exception if any error occurs.
   */
  @Disabled("Performance test")
  @Test
  public void testLoaderLargeInsert() throws Exception {
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
  public void testLoaderInsert() throws Exception {
    // mostly just populate test data but with delay injection to test
    // PUT retry
    new TestDataConfigBuilder(testConnection, putConnection).setTestMode(true).populate();
  }

  @Test
  public void testLoadTime() throws Exception {
    String tableName = "LOADER_TIME_TEST";
    try {
      testConnection
          .createStatement()
          .execute(
              String.format(
                  "CREATE OR REPLACE TABLE %s (" + "ID int, " + "C1 time, C2 date)", tableName));

      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
      tdcb.setTableName(tableName)
          .setStartTransaction(true)
          .setTruncateTable(true)
          .setColumns(Arrays.asList("ID", "C1", "C2"))
          .setKeys(Collections.singletonList("ID"));
      StreamLoader loader = tdcb.getStreamLoader();
      TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
      loader.start();
      Time tm = new Time(3723000);
      Date dt = new Date();
      for (int i = 0; i < 10; i++) {
        Object[] row = new Object[] {i, tm, dt};
        loader.submitRow(row);
      }
      loader.finish();
      String errorMessage = "";
      if (listener.getErrorCount() > 0) {
        errorMessage = listener.getErrors().get(0).getException().toString();
      }
      assertThat(String.format("Error: %s", errorMessage), listener.getErrorCount(), equalTo(0));
      try (Statement statement = testConnection.createStatement()) {
        try (ResultSet rs =
            statement.executeQuery(String.format("SELECT c1, c2 FROM %s LIMIT 1", tableName))) {
          assertTrue(rs.next());
          Time rsTm = rs.getTime(1);
          Date rsDt = rs.getDate(2);
          assertThat("Time column didn't match", rsTm, equalTo(tm));

          Calendar cal = cutOffTimeFromDate(dt);
          long dtEpoch = cal.getTimeInMillis();
          long rsDtEpoch = rsDt.getTime();
          assertThat("Date column didn't match", rsDtEpoch, equalTo(dtEpoch));
        }
      }
    } finally {
      testConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", tableName));
    }
  }

  private Calendar cutOffTimeFromDate(Date dt) {
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
  public void testLoaderDelete() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbDelete = new TestDataConfigBuilder(testConnection, putConnection);
    tdcbDelete
        .setOperation(Operation.DELETE)
        .setTruncateTable(false)
        .setColumns(Arrays.asList("ID", "C1"))
        .setKeys(Arrays.asList("ID", "C1"));
    StreamLoader loader = tdcbDelete.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbDelete.getListener();
    loader.start();

    Object[] del =
        new Object[] {
          42, "foo_42" // deleted
        };
    loader.submitRow(del);

    del =
        new Object[] {
          41, "blah" // ignored, and should not raise any error/warning
        };
    loader.submitRow(del);
    loader.finish();

    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));
    assertThat("submitted row count", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("processed", listener.processed.get(), equalTo(1));
    assertThat("deleted rows", listener.deleted.get(), equalTo(1));
  }

  @Test
  public void testLoaderModify() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbModify = new TestDataConfigBuilder(testConnection, putConnection);
    tdcbModify
        .setOperation(Operation.MODIFY)
        .setTruncateTable(false)
        .setColumns(Arrays.asList("ID", "C1", "C2", "C3", "C4", "C5"))
        .setKeys(Collections.singletonList("ID"));
    StreamLoader loader = tdcbModify.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbModify.getListener();
    loader.start();

    Object[] mod = new Object[] {41, "modified", "some\nthi\"ng\\", 41.6, new Date(), "{}"};
    loader.submitRow(mod);
    mod = new Object[] {40, "modified", "\"something,", 40.2, new Date(), "{}"};
    loader.submitRow(mod);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    // Test deletion
    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT COUNT(*) AS N" + " FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("count is not correct", rs.getInt("N"), equalTo(10000));

    rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT C1 AS N" + " FROM \"%s\" WHERE ID=40", TARGET_TABLE_NAME));

    rs.next();
    assertThat("status is not correct", rs.getString("N"), equalTo("modified"));

    rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT C1, C2" + " FROM \"%s\" WHERE ID=41", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C1 is not correct", rs.getString("C1"), equalTo("modified"));
    assertThat("C2 is not correct", rs.getString("C2"), equalTo("some\nthi\"ng\\"));
  }

  @Test
  public void testLoaderModifyWithOneMatchOneNot() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbModify = new TestDataConfigBuilder(testConnection, putConnection);
    tdcbModify
        .setTruncateTable(false)
        .setOperation(Operation.MODIFY)
        .setColumns(Arrays.asList("ID", "C1", "C2", "C3", "C4", "C5"))
        .setKeys(Collections.singletonList("ID"));
    StreamLoader loader = tdcbModify.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbModify.getListener();
    loader.start();

    Object[] mod = new Object[] {20000, "modified", "some\nthi\"ng\\", 41.6, new Date(), "{}"};
    loader.submitRow(mod);
    mod = new Object[] {45, "modified", "\"something2,", 40.2, new Date(), "{}"};
    loader.submitRow(mod);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(1));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated", listener.updated.get(), equalTo(1));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    // Test deletion
    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT COUNT(*) AS N" + " FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("count is not correct", rs.getInt("N"), equalTo(10000));

    rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT C1, C2" + " FROM \"%s\" WHERE ID=45", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C1 is not correct", rs.getString("C1"), equalTo("modified"));
    assertThat("C2 is not correct", rs.getString("C2"), equalTo("\"something2,"));
  }

  @Test
  public void testLoaderUpsertWithError() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setColumns(Arrays.asList("ID", "C1", "C2", "C3", "C4", "C5"))
        .setKeys(Collections.singletonList("ID"));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    loader.start();

    Object[] upse = new Object[] {"10001-", "inserted", "something", "42-", new Date(), "{}"};
    loader.submitRow(upse);
    upse = new Object[] {10002, "inserted", "something", 43, new Date(), "{}"};
    loader.submitRow(upse);
    upse = new Object[] {45, "modified", "something", 46.1, new Date(), "{}"};
    loader.submitRow(upse);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(3));
    assertThat("counter", listener.counter.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(3));
    assertThat("updated/inserted", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(2));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(1));
    assertThat(
        "Target table name is not correct",
        listener.getErrors().get(0).getTarget(),
        equalTo(TARGET_TABLE_NAME));

    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT COUNT(*) AS N" + " FROM \"%s\"", TARGET_TABLE_NAME));

    rs.next();
    int c = rs.getInt("N");
    assertThat("N is not correct", c, equalTo(10001));

    rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT C1 AS N" + " FROM \"%s\" WHERE ID=45", TARGET_TABLE_NAME));

    rs.next();
    assertThat("N is not correct", rs.getString("N"), equalTo("modified"));
  }

  @Test
  public void testEmptyFieldAsEmpty() throws Exception {
    _testEmptyFieldAsEmpty(true);
    _testEmptyFieldAsEmpty(false);
  }

  private void _testEmptyFieldAsEmpty(boolean copyEmptyFieldAsEmpty) throws Exception {
    String tableName = "LOADER_EMPTY_FIELD_AS_NULL";
    try {
      testConnection
          .createStatement()
          .execute(
              String.format(
                  "CREATE OR REPLACE TABLE %s (" + "ID int, " + "C1 string, C2 string)",
                  tableName));

      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
      tdcb.setOperation(Operation.INSERT)
          .setStartTransaction(true)
          .setTruncateTable(true)
          .setTableName(tableName)
          .setCopyEmptyFieldAsEmpty(copyEmptyFieldAsEmpty)
          .setColumns(Arrays.asList("ID", "C1", "C2"));
      StreamLoader loader = tdcb.getStreamLoader();
      TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
      loader.start();

      // insert null
      loader.submitRow(new Object[] {1, null, ""});
      loader.finish();
      int submitted = listener.getSubmittedRowCount();
      assertThat("submitted rows", submitted, equalTo(1));

      ResultSet rs =
          testConnection
              .createStatement()
              .executeQuery(String.format("SELECT C1, C2 FROM %s", tableName));

      rs.next();
      String c1 = rs.getString(1); // null
      String c2 = rs.getString(2); // empty
      if (!copyEmptyFieldAsEmpty) {
        // COPY ... EMPTY_FIELD_AS_NULL = TRUE /* default */
        assertThat(c1, is(nullValue())); // null => null
        assertThat(c2, equalTo("")); // empty => empty
      } else {
        // COPY EMPTY_FIELD_AS_NULL = FALSE
        assertThat(c1, equalTo("")); // null => empty
        assertThat(c2, equalTo("")); // empty => empty
      }
      rs.close();
    } finally {
      testConnection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", tableName));
    }
  }

  /**
   * Test a target table name including spaces.
   *
   * @throws Exception raises if any error occurs
   */
  @Test
  public void testSpacesInColumnTable() throws Exception {
    String targetTableName = "Load Test Spaces In Columns";

    // create table with spaces in column names
    testConnection
        .createStatement()
        .execute(
            String.format(
                "CREATE OR REPLACE TABLE \"%s\" (" + "ID int, " + "\"Column 1\" varchar(255))",
                targetTableName));

    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    tdcb.setTableName(targetTableName).setColumns(Arrays.asList("ID", "Column 1"));

    StreamLoader loader = tdcb.getStreamLoader();
    loader.start();

    for (int i = 0; i < 5; ++i) {
      Object[] row = new Object[] {i, "foo_" + i};
      loader.submitRow(row);
    }
    loader.finish();

    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("SELECT * FROM \"%s\" ORDER BY \"Column 1\"", targetTableName));

    rs.next();
    assertThat("The first id", rs.getInt(1), equalTo(0));
    assertThat("The first str", rs.getString(2), equalTo("foo_0"));
  }

  @Test
  public void testLoaderInsertAbortStatement() throws Exception {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    StreamLoader loader = tdcb.setOnError("ABORT_STATEMENT").getStreamLoader();
    listener.throwOnError = true;

    loader.start();
    Random rnd = new Random();

    // generates a new data set and ingest
    for (int i = 0; i < 10; i++) {
      final String json = "{\"key\":" + String.valueOf(rnd.nextInt()) + "," + "\"bar\":" + i + "}";
      Object v = rnd.nextInt() / 3;
      if (i == 7) {
        v = "INVALID_INTEGER";
      }
      Object[] row = new Object[] {i, "foo_" + i, v, new Date(), json};
      loader.submitRow(row);
    }
    Loader.DataError ex = assertThrows(Loader.DataError.class, loader::finish);
    assertThat(ex.toString(), containsString("INVALID_INTEGER"));
  }

  @Test
  public void testLoadTimestampMilliseconds() throws Exception {
    String srcTable = "LOAD_TIMESTAMP_MS_SRC";
    String dstTable = "LOAD_TIMESTAMP_MS_DST";

    testConnection
        .createStatement()
        .execute(
            String.format("create or replace table %s(c1 int, c2 timestamp_ntz(9))", srcTable));
    testConnection
        .createStatement()
        .execute(String.format("create or replace table %s like %s", dstTable, srcTable));
    testConnection
        .createStatement()
        .execute(
            String.format(
                "insert into %s(c1,c2) values"
                    + "(1, '2018-05-12 12:34:56.123456789'),"
                    + "(2, '2018-05-13 03:45:27.988'),"
                    + "(3, '2018-05-14 07:12:34'),"
                    + "(4, '2018-12-15 10:43:45.000000876')",
                srcTable));

    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(testConnection, putConnection);
    StreamLoader loader =
        tdcb.setOnError("ABORT_STATEMENT")
            .setSchemaName(SCHEMA_NAME)
            .setTableName(dstTable)
            .setPreserveStageFile(true)
            .setColumns(Arrays.asList("C1", "C2"))
            .getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    listener.throwOnError = true;

    loader.start();

    ResultSet rs =
        testConnection.createStatement().executeQuery(String.format("select * from %s", srcTable));
    while (rs.next()) {
      Object c1 = rs.getObject(1);
      Object c2 = rs.getObject(2);
      Object[] row = new Object[] {c1, c2};
      loader.submitRow(row);
    }
    loader.finish();

    rs =
        testConnection
            .createStatement()
            .executeQuery(
                String.format("select * from %s minus select * from %s", dstTable, srcTable));
    assertThat("No result", !rs.next());
  }
}
