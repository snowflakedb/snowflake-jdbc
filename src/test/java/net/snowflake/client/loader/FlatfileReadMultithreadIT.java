package net.snowflake.client.loader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.LOADER)
public class FlatfileReadMultithreadIT {
  private final int NUM_RECORDS = 100000;

  private static final String TARGET_STAGE = "STAGE_MULTITHREAD_LOADER";
  private static String TARGET_SCHEMA;
  private static String TARGET_DB;

  @BeforeAll
  public static void setUpClass() throws Throwable {
    try (Connection testConnection = AbstractDriverIT.getConnection();
        // NOTE: the stage object must be created right after the connection
        // because the Loader API assumes the stage object exists in the default
        // namespace of the connection.
        Statement statement = testConnection.createStatement()) {
      statement.execute(String.format("CREATE OR REPLACE STAGE %s", TARGET_STAGE));
      TARGET_SCHEMA = testConnection.getSchema();
      TARGET_DB = testConnection.getCatalog();
    }
  }

  @AfterAll
  public static void tearDownClass() throws Throwable {
    try (Connection testConnection = AbstractDriverIT.getConnection();
        Statement statement = testConnection.createStatement()) {
      statement.execute(String.format("DROP STAGE IF EXISTS %s", TARGET_STAGE));
    }
  }

  /**
   * Run loadAPI concurrently to ensure no race condition occurs.
   *
   * @throws Throwable raises an exception if any error occurs.
   */
  @Test
  public void testIssueSimpleDateFormat() throws Throwable {
    final String targetTable = "TABLE_ISSUE_SIMPLEDATEFORMAT";
    try (Connection testConnection = AbstractDriverIT.getConnection();
        Statement statement = testConnection.createStatement()) {
      try {
        statement.execute(
            String.format(
                "CREATE OR REPLACE TABLE %s.%s.%s (" + "ID int, " + "C1 timestamp)",
                TARGET_DB, TARGET_SCHEMA, targetTable));
        Thread t1 =
            new Thread(
                new FlatfileRead(NUM_RECORDS, TARGET_DB, TARGET_SCHEMA, TARGET_STAGE, targetTable));
        Thread t2 =
            new Thread(
                new FlatfileRead(NUM_RECORDS, TARGET_DB, TARGET_SCHEMA, TARGET_STAGE, targetTable));

        t1.start();
        t2.start();
        t1.join();
        t2.join();
        try (ResultSet rs =
            statement.executeQuery(
                String.format(
                    "select count(*) from %s.%s.%s", TARGET_DB, TARGET_SCHEMA, targetTable))) {
          rs.next();
          assertThat("total number of records", rs.getInt(1), equalTo(NUM_RECORDS * 2));
        }

      } finally {
        statement.execute(
            String.format("DROP TABLE IF EXISTS %s.%s.%s", TARGET_DB, TARGET_SCHEMA, targetTable));
      }
    }
  }

  class FlatfileRead implements Runnable {
    private final int totalRows;
    private final String dbName;
    private final String schemaName;
    private final String tableName;
    private final String stageName;

    FlatfileRead(
        int totalRows, String dbName, String schemaName, String stageName, String tableName) {
      this.totalRows = totalRows;
      this.dbName = dbName;
      this.schemaName = schemaName;
      this.stageName = stageName;
      this.tableName = tableName;
    }

    @Override
    public void run() {
      try (Connection testConnection = AbstractDriverIT.getConnection();
          Connection putConnection = AbstractDriverIT.getConnection()) {

        ResultListener _resultListener = new ResultListener();

        // init properties
        Map<LoaderProperty, Object> prop = new HashMap<>();
        prop.put(LoaderProperty.tableName, this.tableName);
        prop.put(LoaderProperty.schemaName, this.schemaName);
        prop.put(LoaderProperty.databaseName, this.dbName);
        prop.put(LoaderProperty.remoteStage, this.stageName);
        prop.put(LoaderProperty.operation, Operation.INSERT);

        StreamLoader underTest =
            (StreamLoader) LoaderFactory.createLoader(prop, putConnection, testConnection);
        underTest.setProperty(LoaderProperty.startTransaction, true);
        underTest.setProperty(LoaderProperty.truncateTable, false);

        underTest.setProperty(LoaderProperty.columns, Arrays.asList("ID", "C1"));

        underTest.setListener(_resultListener);
        underTest.start();

        Random rnd = new Random();
        for (int i = 0; i < this.totalRows; ++i) {
          Object[] row = new Object[2];
          row[0] = i;
          // random timestamp data
          long ms = -946771200000L + (Math.abs(rnd.nextLong()) % (70L * 365 * 24 * 60 * 60 * 1000));
          row[1] = new Date(ms);
          underTest.submitRow(row);
        }

        try {
          underTest.finish();
        } catch (Exception e) {
          e.printStackTrace();
        }
        underTest.close();
        assertThat("must be no error", _resultListener.getErrorCount(), equalTo(0));
        assertThat(
            "total number of rows",
            _resultListener.getSubmittedRowCount(),
            equalTo(this.totalRows));
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    class ResultListener implements LoadResultListener {

      private final List<LoadingError> errors = new ArrayList<>();

      private final AtomicInteger errorCount = new AtomicInteger(0);
      private final AtomicInteger errorRecordCount = new AtomicInteger(0);

      private final AtomicInteger counter = new AtomicInteger(0);
      private final AtomicInteger processed = new AtomicInteger(0);
      private final AtomicInteger deleted = new AtomicInteger(0);
      private final AtomicInteger updated = new AtomicInteger(0);
      private final AtomicInteger submittedRowCount = new AtomicInteger(0);

      private Object[] lastRecord = null;

      public boolean throwOnError = false; // should not trigger rollback

      @Override
      public boolean needErrors() {
        return true;
      }

      @Override
      public boolean needSuccessRecords() {
        return true;
      }

      @Override
      public void addError(LoadingError error) {
        errors.add(error);
      }

      @Override
      public boolean throwOnError() {
        return throwOnError;
      }

      public List<LoadingError> getErrors() {
        return errors;
      }

      @Override
      public void recordProvided(Operation op, Object[] record) {
        lastRecord = record;
      }

      @Override
      public void addProcessedRecordCount(Operation op, int i) {
        processed.addAndGet(i);
      }

      @Override
      public void addOperationRecordCount(Operation op, int i) {
        counter.addAndGet(i);
        if (op == Operation.DELETE) {
          deleted.addAndGet(i);
        } else if (op == Operation.MODIFY || op == Operation.UPSERT) {
          updated.addAndGet(i);
        }
      }

      public Object[] getLastRecord() {
        return lastRecord;
      }

      @Override
      public int getErrorCount() {
        return errorCount.get();
      }

      @Override
      public int getErrorRecordCount() {
        return errorRecordCount.get();
      }

      @Override
      public void resetErrorCount() {
        errorCount.set(0);
      }

      @Override
      public void resetErrorRecordCount() {
        errorRecordCount.set(0);
      }

      @Override
      public void addErrorCount(int count) {
        errorCount.addAndGet(count);
      }

      @Override
      public void addErrorRecordCount(int count) {
        errorRecordCount.addAndGet(count);
      }

      @Override
      public void resetSubmittedRowCount() {
        submittedRowCount.set(0);
      }

      @Override
      public void addSubmittedRowCount(int count) {
        submittedRowCount.addAndGet(count);
      }

      @Override
      public int getSubmittedRowCount() {
        return submittedRowCount.get();
      }
    }
  }
}
