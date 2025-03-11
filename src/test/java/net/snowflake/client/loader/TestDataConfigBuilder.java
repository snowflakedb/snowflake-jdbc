package net.snowflake.client.loader;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;

class TestDataConfigBuilder {
  static final String TARGET_TABLE_NAME = "LOADER_test_TABLE";

  private StreamLoader streamLoader;
  private final Connection testConnection;
  private final Connection putConnection;

  private Operation operation = Operation.INSERT;
  private boolean testMode = false;
  private int numberOfRows = 10000;
  private List<Object[]> dataSet;

  private List<String> columns = Arrays.asList("ID", "C1", "C3", "C4", "C5");
  private List<String> keys;
  private String tableName = TARGET_TABLE_NAME;
  private String schemaName;
  private String databaseName;
  private String remoteStage = "~";
  private long csvFileBucketSize = 64;
  private long csvFileSize = 50 * 1024 * 1024;
  private String onError = OnError.DEFAULT;
  private boolean startTransaction = false;
  private boolean truncateTable = true;
  private boolean preserveStageFile = false;
  private boolean useLocalTimezone = false;
  private boolean mapTimeToTimestamp = false;
  private boolean copyEmptyFieldAsEmpty = false;
  private boolean compressFileByPut = false;
  private boolean compressDataBeforePut = true;
  private long compressLevel = 1L;

  private ResultListener listener;

  TestDataConfigBuilder(Connection testConnection, Connection putConnection) throws Exception {
    this.testConnection = testConnection;
    this.putConnection = putConnection;
    this.databaseName = testConnection.getCatalog();
    this.schemaName = testConnection.getSchema();
  }

  TestDataConfigBuilder setTestMode(boolean testMode) {
    this.testMode = testMode;
    return this;
  }

  TestDataConfigBuilder setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  TestDataConfigBuilder setSchemaName(String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  TestDataConfigBuilder setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  TestDataConfigBuilder setRemoteStage(String remoteStage) {
    this.remoteStage = remoteStage;
    return this;
  }

  TestDataConfigBuilder setCsvFileBucketSize(long csvFileBucketSize) {
    this.csvFileBucketSize = csvFileBucketSize;
    return this;
  }

  TestDataConfigBuilder setCsvFileSize(long csvFileSize) {
    this.csvFileSize = csvFileSize;
    return this;
  }

  TestDataConfigBuilder setOnError(String onError) {
    this.onError = onError;
    return this;
  }

  TestDataConfigBuilder setNumberOfRows(int numberOfRows) {
    this.numberOfRows = numberOfRows;
    return this;
  }

  TestDataConfigBuilder setDataSet(List<Object[]> dataSet) {
    this.dataSet = dataSet;
    return this;
  }

  TestDataConfigBuilder setColumns(List<String> columns) {
    this.columns = columns;
    return this;
  }

  TestDataConfigBuilder setKeys(List<String> keys) {
    this.keys = keys;
    return this;
  }

  TestDataConfigBuilder setOperation(Operation operation) {
    this.operation = operation;
    return this;
  }

  TestDataConfigBuilder setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
    return this;
  }

  TestDataConfigBuilder setPreserveStageFile(boolean preserveStageFile) {
    this.preserveStageFile = preserveStageFile;
    return this;
  }

  TestDataConfigBuilder setUseLocalTimezone(boolean useLocalTimezone) {
    this.useLocalTimezone = useLocalTimezone;
    return this;
  }

  TestDataConfigBuilder setMapTimeToTimestamp(boolean mapTimeToTimestamp) {
    this.mapTimeToTimestamp = mapTimeToTimestamp;
    return this;
  }

  TestDataConfigBuilder setStartTransaction(boolean startTransaction) {
    this.startTransaction = startTransaction;
    return this;
  }

  TestDataConfigBuilder setCopyEmptyFieldAsEmpty(boolean copyEmptyFieldAsEmpty) {
    this.copyEmptyFieldAsEmpty = copyEmptyFieldAsEmpty;
    return this;
  }

  TestDataConfigBuilder setCompressFileByPut(boolean compressFileByPut) {
    this.compressFileByPut = compressFileByPut;
    return this;
  }

  TestDataConfigBuilder setCompressDataBeforePut(boolean compressDataBeforePut) {
    this.compressDataBeforePut = compressDataBeforePut;
    return this;
  }

  TestDataConfigBuilder setCompressLevel(long compressLevel) {
    this.compressLevel = compressLevel;
    return this;
  }

  StreamLoader getStreamLoader() throws Exception {
    getListener();
    return streamLoader;
  }

  synchronized ResultListener getListener() throws Exception {
    if (listener == null) {
      Map<LoaderProperty, Object> prop = this.initLoaderProperties();
      listener = this.initLoader(prop);
    }
    return listener;
  }

  void populate() throws Exception {
    populate(false);
  }

  List<Object[]> populateReturnData() throws Exception {
    return populate(true);
  }

  List<Object[]> populate(boolean returnDataSet) throws Exception {
    getListener();
    streamLoader.start();
    Random rnd = new Random();

    List<Object[]> newDataSet = new ArrayList<>();
    if (dataSet == null) {
      // generates a new data set and ingest
      for (int i = 0; i < numberOfRows; i++) {
        final String json = "{\"key\":" + rnd.nextInt() + "," + "\"bar\":" + i + "}";
        Object[] row = new Object[] {i, "foo_" + i, rnd.nextInt() / 3, new Date(), json};
        if (returnDataSet) {
          newDataSet.add(row);
        }
        streamLoader.submitRow(row);
      }
    } else {
      for (Object[] row : dataSet) {
        // ingest the same data set
        streamLoader.submitRow(row);
      }
    }
    streamLoader.finish();
    int submitted = listener.getSubmittedRowCount();

    assertThat("submitted rows", submitted, equalTo(numberOfRows));
    assertThat(
        "_resultListener.counter is not correct", listener.counter.get(), equalTo(numberOfRows));
    assertThat("_resultListener.getErrors() was not 0", listener.getErrors().size(), equalTo(0));

    ResultSet rs =
        testConnection
            .createStatement()
            .executeQuery(String.format("SELECT COUNT(*) AS N" + " FROM \"%s\"", tableName));

    rs.next();
    int count = rs.getInt("N");
    assertThat("count is not correct", count, equalTo(numberOfRows));
    assertThat(
        "_resultListener.processed didn't match count", listener.processed.get(), equalTo(count));
    assertThat(
        "_resultListener.counter didn't match count", listener.counter.get(), equalTo(count));
    assertThat(
        "_resultListener.getErrors().size() was not 0", listener.getErrors().size(), equalTo(0));
    assertThat(
        "_resultListener.getLastRecord()[0] was not 9999",
        (Integer) listener.getLastRecord()[0],
        equalTo(numberOfRows - 1));
    return newDataSet;
  }

  private Map<LoaderProperty, Object> initLoaderProperties() {
    Map<LoaderProperty, Object> prop = new HashMap<>();
    prop.put(LoaderProperty.tableName, tableName);
    prop.put(LoaderProperty.schemaName, schemaName);
    prop.put(LoaderProperty.databaseName, databaseName);
    prop.put(LoaderProperty.remoteStage, remoteStage);
    prop.put(LoaderProperty.operation, operation);
    prop.put(LoaderProperty.columns, columns);
    prop.put(LoaderProperty.keys, keys);
    return prop;
  }

  private ResultListener initLoader(Map<LoaderProperty, Object> prop) throws Exception {
    ResultListener _resultListener = new ResultListener();

    // Set up Test parameters
    streamLoader = (StreamLoader) LoaderFactory.createLoader(prop, putConnection, testConnection);

    streamLoader.setProperty(LoaderProperty.startTransaction, startTransaction);
    streamLoader.setProperty(LoaderProperty.truncateTable, truncateTable);
    streamLoader.setProperty(LoaderProperty.preserveStageFile, preserveStageFile);
    streamLoader.setProperty(LoaderProperty.useLocalTimezone, useLocalTimezone);
    streamLoader.setProperty(LoaderProperty.mapTimeToTimestamp, mapTimeToTimestamp);
    streamLoader.setProperty(LoaderProperty.copyEmptyFieldAsEmpty, copyEmptyFieldAsEmpty);
    // file bucket size
    streamLoader.setProperty(LoaderProperty.csvFileBucketSize, Long.toString(csvFileBucketSize));
    // file batch
    streamLoader.setProperty(LoaderProperty.csvFileSize, Long.toString(csvFileSize));
    streamLoader.setProperty(LoaderProperty.compressFileByPut, compressFileByPut);
    streamLoader.setProperty(LoaderProperty.compressDataBeforePut, compressDataBeforePut);
    streamLoader.setProperty(LoaderProperty.compressLevel, compressLevel);

    // ON_ERROR option
    streamLoader.setProperty(LoaderProperty.onError, onError);

    streamLoader.setListener(_resultListener);

    // causes upload to fail
    streamLoader.setTestMode(testMode);

    // Wait for 5 seconds on first put to buffer everything up.
    putConnection.unwrap(SnowflakeConnectionV1.class).setInjectedDelay(5000);

    return _resultListener;
  }

  class ResultListener implements LoadResultListener {

    private final List<LoadingError> errors = new ArrayList<>();

    private final AtomicInteger errorCount = new AtomicInteger(0);
    private final AtomicInteger errorRecordCount = new AtomicInteger(0);

    public final AtomicInteger counter = new AtomicInteger(0);
    public final AtomicInteger processed = new AtomicInteger(0);
    public final AtomicInteger deleted = new AtomicInteger(0);
    public final AtomicInteger updated = new AtomicInteger(0);
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
