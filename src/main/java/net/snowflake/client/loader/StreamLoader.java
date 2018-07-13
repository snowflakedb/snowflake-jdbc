/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;

import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Stream Loader
 */
public class StreamLoader implements Loader, Runnable
{
  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(
          StreamLoader.class);

  public final static String FILE_PREFIX = "stream_";

  public final static String FILE_SUFFIX = ".gz";
  
  /**
   * Default batch row size
   */
  public final static long DEFAULT_BATCH_ROW_SIZE = -1L;

  public static DatabaseMetaData metadata;

  private BufferStage _stage = null;

  private Operation _op = null;

  private boolean _startTransaction = false;

  // force not to truncate in case where loader.start is called in 
  // multiple times in a single job.
  private boolean _is_first_start_call = true;

  // force not to commit or rollback in case where loader.finish is called
  // in multiple times in a single job.
  private boolean _is_last_finish_call = true;
    
  private boolean _oneBatch = false;

  private boolean _truncate = false;
  
  private String _before = null;

  private String _after = null;

  private ArrayBlockingQueue<byte[]> _queueData;

  private Thread _thread;

  private ArrayBlockingQueue<BufferStage> _queuePut;

  private PutQueue _put;

  private ArrayBlockingQueue<BufferStage> _queueProcess;

  private ProcessQueue _process;

  private String _remoteStage = "~";

  private String _table;

  private String _schema;

  private String _database;

  private List<String> _columns;

  private List<String> _keys;
  
  private long _batchRowSize = DEFAULT_BATCH_ROW_SIZE;

  private long _csvFileBucketSize = BufferStage.FILE_BUCKET_SIZE;

  private long _csvFileSize = BufferStage.FILE_SIZE;

  boolean _testRemoteBadCSV = false; // TEST: inject bad csv in remote stage

  boolean _preserveStageFile = false; // reserve stage file

  private boolean _useLocalTimezone = false; // use local timezone instead of UTC

  private boolean _mapTimeToTimestamp = false; // map TIME to TIMESTAMP. Informatica V1 connector behavior

  String _onError = OnError.DEFAULT;

  boolean _copyEmptyFieldAsEmpty = false; // COPY command option to set EMPTY_FIELD_AS_NULL = false

  boolean _testMode = false;

  private final Connection _putConn;
  private final Connection _processConn;


  //a per-instance bit of random noise to make make filenames more unique
  private final String _noise;


  // Track fatal errors
  private AtomicBoolean _active = new AtomicBoolean(false);
  private AtomicBoolean _aborted = new AtomicBoolean(false);
  private RuntimeException _abortCause =  new ConnectionError(
          "Unknown exception");

  private AtomicInteger _throttleCounter = new AtomicInteger(0);

  public StreamLoader(Map<LoaderProperty, Object> properties,
                      Connection putConnection,
                      Connection processConnection)
  {
    _putConn = putConnection;
    _processConn = processConnection;
    for(Map.Entry e: properties.entrySet())
    {
      setProperty((LoaderProperty) e.getKey(), e.getValue());
    }

    _noise = SnowflakeUtil.randomAlphaNumeric(6);
  }

  @Override
  public void setProperty(LoaderProperty property, Object value)
  {
    switch (property)
    {
      case tableName:
        _table = (String) value;
        break;
      case schemaName:
        _schema = (String) value;
        break;
      case databaseName:
        _database = (String) value;
        break;
      case remoteStage:
        _remoteStage = (String) value;
        break;
      case columns:
        _columns = (List<String>) value;
        break;
      case keys:
        _keys = (List<String>) value;
        break;
      case operation:
        _op = (Operation) value;
        break;
      case startTransaction:
        _startTransaction = Boolean.valueOf(String.valueOf(value));
        break;
      case oneBatch:
        _oneBatch = Boolean.valueOf(String.valueOf(value));
        break;
      case truncateTable:
        _truncate = Boolean.valueOf(String.valueOf(value));
        break;
      case executeBefore:
        _before = String.valueOf(value);
        break;
      case executeAfter:
        _after = String.valueOf(value);
        break;
      case isFirstStartCall:
         _is_first_start_call = Boolean.valueOf(String.valueOf(value));
        break;
      case isLastFinishCall:
        _is_last_finish_call = Boolean.valueOf(String.valueOf(value));
        break;
      case batchRowSize:
        if (value instanceof String) {
          _batchRowSize = new Long((String) value);
        } else if (value instanceof Long){
          _batchRowSize = (Long)value;
        }
        break;
      case csvFileBucketSize:
        if (value instanceof String) {
          _csvFileBucketSize = new Long((String) value);
        } else if (value instanceof Long){
          _csvFileBucketSize = (Long)value;
        }
        break;
      case csvFileSize:
        if (value instanceof String) {
          _csvFileSize = new Long((String) value);
        } else if (value instanceof Long){
          _csvFileSize = (Long)value;
        }
        break;
      case preserveStageFile:
        _preserveStageFile = Boolean.valueOf(String.valueOf(value));
        break;
      case useLocalTimezone:
        _useLocalTimezone = Boolean.valueOf(String.valueOf(value));
        break;
      case copyEmptyFieldAsEmpty:
        _copyEmptyFieldAsEmpty = Boolean.valueOf(String.valueOf(value));
        break;
      case mapTimeToTimestamp:
        // NOTE: this is a special flag to change mapping
        // from TIME. Informatica connector v1 maps to TIMESTAMP
        // but a legitimate behavior is supposed to be to TIME.
        _mapTimeToTimestamp = Boolean.valueOf(String.valueOf(value));
        break;
      case onError:
        String v = String.valueOf(value);
        _onError = OnError.validate(v) ? v : OnError.DEFAULT;
        break;
      case testRemoteBadCSV:
        _testRemoteBadCSV = Boolean.valueOf(String.valueOf(value));
        break;
      default:
        // nop, this should ever happens
    }
  }

  /**
   * Starts the loader
   */
  @Override
  public void start()
  {
    LOGGER.debug("Start Loading");
    // validate parameters
    validateParameters();

    if (_op == null) {
      this.abort(new ConnectionError("Loader started with no operation"));
      return;
    }
    
    initQueues();

    if (_is_first_start_call) {
      // is this the first start call?

      try {
        if (_startTransaction) {
          LOGGER.info("Begin Transaction");
          _processConn.createStatement().execute("begin transaction");
        } else {
          LOGGER.info("No Transaction started");
        }
      } catch (SQLException ex) {
        abort(new Loader.ConnectionError("Failed to start Transaction",
                Utils.getCause(ex)));
      }

      if (_truncate) {
        truncateTargetTable();
      }

      try {
        if(_before != null) {
          LOGGER.info("Running Execute Before SQL");
          _processConn.createStatement().execute(_before);
        }
      }
      catch (SQLException ex) {
        abort(new Loader.ConnectionError(
                String.format("Execute Before SQL failed to run: %s", _before),
                Utils.getCause(ex)));
      }
    }
  }

  private void validateParameters() {
    LOGGER.debug("Validate Parameters");
    if(Operation.INSERT != this._op) {
      if(this._keys == null || this._keys.isEmpty()) {
        throw new ConnectionError("Updating operations require keys");
      }
    }
  }

  String getNoise()
  {
    return _noise;
  }
  
  public void abort(RuntimeException t) {
    synchronized(this) {
      // Abort once, keep first error.
      LOGGER.warn("Exception received. Aborting...", t);

      if (_aborted.getAndSet(true)) {
        return;
      }

      if (t != null) {
        _abortCause = t;
      }

      // Rollback and do not process anything else
      rollback();
    }
  }

  boolean isAborted() {
    synchronized(this) {
      // don't synchronize unless the caller does
      return _aborted.get();
    }
  }

  @Override
  public void rollback() {
    LOGGER.debug("Rollback");
    try {
      terminate();

      LOGGER.warn("Rollback");
      this._processConn.createStatement().execute("rollback");
    }
    catch (SQLException ex) {
      LOGGER.error(ex.getMessage(), ex);
    }
  }

  @Override
  public void submitRow(Object[] row) {
    try {
      if (_aborted.get()) {
        if (_listener.throwOnError()) {
          throw _abortCause;
        }
        return;
      }
    } catch (Exception ex) {
      abort(new Loader.ConnectionError(
              "Throwing Error", Utils.getCause(ex)));
    }

    byte[] data = null;
    try {
      if (!_active.get()) {
        LOGGER.warn("Inactive loader. Row ignored");
        return;
      }

      data = createCSVRecord(row);

    } catch (Exception ex) {
      abort(new Loader.ConnectionError(
              "Creating data set for CSV", Utils.getCause(ex)));
    }

    try {
      writeBytes(data);
      _listener.addSubmittedRowCount(1);

      if (_listener.needSuccessRecords()) {
        _listener.recordProvided(_op, row);
      }
    } catch (Exception ex) {
      abort(new Loader.ConnectionError(
              "Writing Bytes to CSV files", Utils.getCause(ex)));
    }

    if (_batchRowSize > 0 && _listener.getSubmittedRowCount() > 0 &&
        (_listener.getSubmittedRowCount() % _batchRowSize) == 0) {
      LOGGER.debug("Flushing Queue: Submitted Row Count: {}, Batch Row Size: {}",
          _listener.getSubmittedRowCount(), _batchRowSize);
      // flush data loading
      try {
        flushQueues();
      } catch (Exception ex) {
        abort(new Loader.ConnectionError(
                "Flush Queues", Utils.getCause(ex)));
      }
      try {
        initQueues();
      } catch (Exception ex) {
        abort(new Loader.ConnectionError(
                "Init Queues", Utils.getCause(ex)));
      }
    }
  }

  /**
   * Initializes queues
   * 
   */
  private void initQueues() {
    LOGGER.debug("Init Queues");
    if (_active.getAndSet(true)) {
      // NOP if the loader is already active
      return;
    }
    // start PUT and PROCESS queues
    _queuePut = new ArrayBlockingQueue<>(48);
    _queueProcess = new ArrayBlockingQueue<>(48);
    _put = new PutQueue(this);
    _process = new ProcessQueue(this);

    // Start queue. NOTE: This is not actively used
    _queueData = new ArrayBlockingQueue<>(1024);
    _thread = new Thread(this);
    _thread.setName("StreamLoaderThread");
    _thread.start();

    // Create stage
    _stage = new BufferStage(this, _op, _csvFileBucketSize, _csvFileSize);
  }

  /**
   * Flushes data by joining PUT and PROCESS queues 
   */
  private void flushQueues() {
    // Terminate data loading thread.
    LOGGER.debug("Flush Queues");
    try
    {
      _queueData.put(new byte[0]);
      _thread.join(10000);

      if (_thread.isAlive())
      {
        _thread.interrupt();
      }
    }
    catch (Exception ex)
    {
      String msg = "Failed to join StreamLoader queue: " + ex.getMessage();
      LOGGER.error(msg, ex);
      throw new DataError(msg, Utils.getCause(ex));
    }
    // Put last stage on queue
    terminate();

    // wait for the processing to finish
    _put.join();
    _process.join();

    if (_aborted.get())
    {
      // Loader was aborted due to an exception. 
      // It was rolled back at that time.

      //LOGGER.log(Level.WARNING,
      //            "Loader had been previously aborted by error", _abortCause);
      throw _abortCause;
    }
  }
  
  private void writeBytes(byte[] data) throws IOException, InterruptedException
  {
    // this loader was aborted
    if (_aborted.get())
    {
      return;
    }

    boolean full = _stage.stageData(data, true);

    if (full && !_oneBatch)
    {
      // if Buffer stage is full and NOT one batch mode,
      // queue PUT request.
      queuePut(_stage);
      _stage = new BufferStage(this, _op, _csvFileBucketSize, _csvFileSize);
    }
  }


  private void truncateTargetTable() {
    try {
      // TODO: could be replaced with TRUNCATE?
      _processConn.createStatement().execute(
              "DELETE FROM " + this.getFullTableName());
    } catch (SQLException ex) {
      LOGGER.error(ex.getMessage(), ex);
      abort(new Loader.ConnectionError(Utils.getCause(ex)));
    }
  }

  @Override
  public void run()
  {
    try {
      while (true) {
        byte[] data = this._queueData.take();

        if (data.length == 0) {
          break;
        }

        this.writeBytes(data);
      }
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      abort(new Loader.ConnectionError(Utils.getCause(ex)));
    }
  }

  private byte[] createCSVRecord(Object[] data)
  {
    StringBuilder sb = new StringBuilder(1024);

    for (int i = 0; i < data.length; ++i)
    {
      if(i > 0) sb.append(',');
      sb.append(SnowflakeType.escapeForCSV(
              SnowflakeType.lexicalValue(data[i],
                  _useLocalTimezone, _mapTimeToTimestamp)));
    }
    return sb.toString().getBytes(UTF_8);
  }

  /**
   * Finishes loader
   * 
   * @throws Exception an exception raised in finishing loader.
   */
  @Override
  public void finish() throws Exception
  {
    LOGGER.debug("Finish Loading");
    flushQueues();

    if (_is_last_finish_call) {
      try
      {
        if(_after != null)
        {
          LOGGER.info("Running Execute After SQL");
          _processConn.createStatement().execute(_after);
        }
        // Loader sucessfully completed. Commit and return.
        _processConn.createStatement().execute("commit");
        LOGGER.info("Committed");
      }
      catch (SQLException ex) {
        try {
          _processConn.createStatement().execute("rollback");
        } catch(SQLException ex0) {
          LOGGER.warn("Failed to rollback");
        }
        LOGGER.warn(String.format("Execute After SQL failed to run: %s", _after),
                ex);
        throw new Loader.ConnectionError(Utils.getCause(ex));
      }
    }
  }

  @Override
  public void close()
  {
    LOGGER.info("Close Loader");
    try
    {
      this._processConn.close();
      this._putConn.close();
    }
    catch (SQLException ex)
    {
      LOGGER.error(ex.getMessage(), ex);
      throw new ConnectionError(Utils.getCause(ex));
    }
  }

  /**
   * Set active to false (no-op if not active), add a stage with terminate flag
   * onto the queue
   */
  private void terminate()
  {
    LOGGER.debug("Terminate Loader");

    boolean active = _active.getAndSet(false);

    if (!active) return;  // No-op

    if (_stage == null) {
      _stage = new BufferStage(this, Operation.INSERT, _csvFileBucketSize, _csvFileSize);
    }

    _stage.setTerminate(true);

    try {
      queuePut(_stage);
    }
    catch (InterruptedException ex) {
      LOGGER.error("Unknown Error", ex);
    }

    LOGGER.debug("Snowflake loader terminating");

  }

  // If operation changes, existing stage needs to be scheduled for processing.
  @Override
  public void resetOperation(Operation op)
  {
    LOGGER.debug("Reset Loader");

    if (op.equals(_op))
    {
      //no-op
      return;
    }

    LOGGER.debug("Operation is changing from {} to {}", _op, op);
    _op = op;

    if (_stage != null)
    {
      try
      {
        queuePut(_stage);
      }
      catch (InterruptedException ex)
      {
        LOGGER.error(_stage.getId(), ex);
      }
    }

    _stage = new BufferStage(this, _op, _csvFileBucketSize, _csvFileSize);

  }

  String getTable()
  {
    return _table;
  }

  String getBase()
  {
    return BASE;
  }

  Connection getPutConnection()
  {
    return _putConn;
  }

  Connection getProcessConnection()
  {
    return _processConn;
  }

  String getRemoteStage()
  {
    return _remoteStage;
  }

  List<String> getKeys()
  {
    return this._keys;
  }

  List<String> getColumns()
  {
    return this._columns;
  }

  String getColumnsAsString()
  {
    // comma separate list of column names
    StringBuilder sb = new StringBuilder("\"");
    for(int i = 0; i < _columns.size(); i++)
    {
      if(i > 0) sb.append("\",\"");
      sb.append(_columns.get(i));
    }
    sb.append("\"");
    return sb.toString();
  }

  String getFullTableName()
  {
    return (_database == null ? "" : ("\"" + _database + "\"."))
           + (_schema == null ? "" : ("\"" + _schema + "\"."))
           + "\"" + _table + "\"";
  }

  public LoadResultListener getListener()
  {
    return _listener;
  }

  @Override
  public void setListener(LoadResultListener _listener)
  {
    this._listener = _listener;
  }

  void queuePut(BufferStage stage) throws InterruptedException
  {
    _queuePut.put(stage);
  }

  BufferStage takePut() throws InterruptedException
  {
    return _queuePut.take();
  }

  void queueProcess(BufferStage stage) throws InterruptedException
  {
    _queueProcess.put(stage);

  }

  BufferStage takeProcess() throws InterruptedException
  {
    return _queueProcess.take();
  }

  int throttleUp()
  {
    int open =  this._throttleCounter.incrementAndGet();
    LOGGER.info("PUT Throttle Up: {}", open);
    if(open > 8) {
      LOGGER.info("Will retry scheduling file for upload after {} seconds",
                        (Math.pow(2, open - 7)));
      try
      {
        Thread.sleep(1000 * ((int)Math.pow(2, open - 7)));
      }
      catch (InterruptedException ex)
      {
        LOGGER.error("Exception occurs while waiting", ex);
      }
    }
    return open;
  }

  int throttleDown()
  {
    int throttleLevel = this._throttleCounter.decrementAndGet();
    LOGGER.debug("PUT Throttle Down: {}", throttleLevel);
    if (throttleLevel < 0)
    {
      LOGGER.warn("Unbalanced throttle");
      _throttleCounter.set(0);
    }

    LOGGER.debug("Connector throttle {}", throttleLevel);
    return throttleLevel;
  }

  private LoadResultListener _listener = new LoadResultListener()
  {

    final private AtomicInteger errorCount = new AtomicInteger(0);
    final private AtomicInteger errorRecordCount = new AtomicInteger(0);
    final private AtomicInteger submittedRowCount = new AtomicInteger(0);
    
    @Override
    public boolean needErrors()
    {
        return false;
    }

    @Override
    public boolean needSuccessRecords()
    {
        return false;
    }

    @Override
    public void addError(LoadingError error)
    {
    }

    @Override
    public boolean throwOnError()
    {
      return false;
    }

    @Override
    public void recordProvided(Operation op, Object[] record)
    {
    }

    @Override
    public void addProcessedRecordCount(Operation op, int i)
    {
    }

    @Override
    public void addOperationRecordCount(Operation op, int i)
    {
    }


    @Override
    public int getErrorCount()
    {
        return errorCount.get();
    }

    @Override
    public int getErrorRecordCount()
    {
        return errorRecordCount.get();
    }

    @Override
    public void resetErrorCount()
    {
        errorCount.set(0);
    }

    @Override
    public void resetErrorRecordCount()
    {
        errorRecordCount.set(0);
    }

    @Override
    public void addErrorCount(int count)
    {
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

  };

  void setTestMode(boolean mode)
  {
    this._testMode = mode;
  }
}
