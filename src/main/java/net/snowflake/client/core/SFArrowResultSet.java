/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.core.StmtUtil.eventHandler;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.TimeZone;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.ArrowResultChunk.ArrowChunkIterator;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.memory.RootAllocator;

/** Arrow result set implementation */
public class SFArrowResultSet extends SFBaseResultSet implements DataConversionContext {
  static final SFLogger logger = SFLoggerFactory.getLogger(SFArrowResultSet.class);

  /** iterator over current arrow result chunk */
  private ArrowChunkIterator currentChunkIterator;

  /** current query id */
  private String queryId;

  /** type of statement generate this result set */
  private SFStatementType statementType;

  private boolean totalRowCountTruncated;

  /** true if sort first chunk */
  private boolean sortResult;

  /** statement generate current result set */
  protected SFStatement statement;

  /** is array bind supported */
  private final boolean arrayBindSupported;

  /** sesion timezone */
  private TimeZone timeZone;

  /** index of next chunk to consume */
  private long nextChunkIndex = 0;

  /** total chunk count, not include first chunk */
  private final long chunkCount;

  /** chunk downloader */
  private ChunkDownloader chunkDownloader;

  /** time when first chunk arrived */
  private final long firstChunkTime;

  /** telemetry client to push stats to server */
  private final Telemetry telemetryClient;

  /**
   * memory allocator for Arrow. Each SFArrowResultSet contains one rootAllocator. This
   * rootAllocactor will be cleared and closed when the resultSet is closed
   */
  private RootAllocator rootAllocator;

  /**
   * If customer wants Timestamp_NTZ values to be stored in UTC time instead of a local/session
   * timezone, set to true
   */
  private boolean treatNTZAsUTC;

  /** Set to true if want to use wallclock time */
  private boolean useSessionTimezone;

  /**
   * If customer wants getDate(int col, Calendar cal) function to format date with Calendar
   * timezone, set to true
   */
  private boolean formatDateWithTimezone;

  /**
   * Constructor takes a result from the API response that we get from executing a SQL statement.
   *
   * <p>The constructor will initialize the ResultSetMetaData.
   *
   * @param resultSetSerializable result data after parsing json
   * @param session SFSession object
   * @param statement statement object
   * @param sortResult true if sort results otherwise false
   * @throws SQLException exception raised from general SQL layers
   */
  SFArrowResultSet(
      SnowflakeResultSetSerializableV1 resultSetSerializable,
      SFSession session,
      SFStatement statement,
      boolean sortResult)
      throws SQLException {
    this(resultSetSerializable, session.getTelemetryClient(), sortResult);

    // update the session db/schema/wh/role etc
    this.statement = statement;
    session.setDatabase(resultSetSerializable.getFinalDatabaseName());
    session.setSchema(resultSetSerializable.getFinalSchemaName());
    session.setRole(resultSetSerializable.getFinalRoleName());
    session.setWarehouse(resultSetSerializable.getFinalWarehouseName());
    treatNTZAsUTC = resultSetSerializable.getTreatNTZAsUTC();
    formatDateWithTimezone = resultSetSerializable.getFormatDateWithTimeZone();
    useSessionTimezone = resultSetSerializable.getUseSessionTimezone();

    // update the driver/session with common parameters from GS
    SessionUtil.updateSfDriverParamValues(this.parameters, statement.getSFBaseSession());

    // if server gives a send time, log time it took to arrive
    if (resultSetSerializable.getSendResultTime() != 0) {
      long timeConsumeFirstResult = this.firstChunkTime - resultSetSerializable.getSendResultTime();
      logMetric(TelemetryField.TIME_CONSUME_FIRST_RESULT, timeConsumeFirstResult);
    }

    eventHandler.triggerStateTransition(
        BasicEvent.QueryState.CONSUMING_RESULT,
        String.format(BasicEvent.QueryState.CONSUMING_RESULT.getArgString(), queryId, 0));
  }

  /**
   * This is a minimum initialization for SFArrowResult. Mainly used for testing purpose. However,
   * real prod constructor will call this constructor as well
   *
   * @param resultSetSerializable data returned in query response
   * @param telemetryClient telemetryClient
   * @throws SQLException
   */
  public SFArrowResultSet(
      SnowflakeResultSetSerializableV1 resultSetSerializable,
      Telemetry telemetryClient,
      boolean sortResult)
      throws SQLException {
    this.resultSetSerializable = resultSetSerializable;
    this.rootAllocator = resultSetSerializable.getRootAllocator();
    this.sortResult = sortResult;
    this.queryId = resultSetSerializable.getQueryId();
    this.statementType = resultSetSerializable.getStatementType();
    this.totalRowCountTruncated = resultSetSerializable.isTotalRowCountTruncated();
    this.parameters = resultSetSerializable.getParameters();
    this.chunkCount = resultSetSerializable.getChunkFileCount();
    this.chunkDownloader = resultSetSerializable.getChunkDownloader();
    this.timeZone = resultSetSerializable.getTimeZone();
    this.honorClientTZForTimestampNTZ = resultSetSerializable.isHonorClientTZForTimestampNTZ();
    this.resultVersion = resultSetSerializable.getResultVersion();
    this.numberOfBinds = resultSetSerializable.getNumberOfBinds();
    this.arrayBindSupported = resultSetSerializable.isArrayBindSupported();
    this.metaDataOfBinds = resultSetSerializable.getMetaDataOfBinds();
    this.telemetryClient = telemetryClient;
    this.firstChunkTime = System.currentTimeMillis();
    this.timestampNTZFormatter = resultSetSerializable.getTimestampNTZFormatter();
    this.timestampLTZFormatter = resultSetSerializable.getTimestampLTZFormatter();
    this.timestampTZFormatter = resultSetSerializable.getTimestampTZFormatter();
    this.dateFormatter = resultSetSerializable.getDateFormatter();
    this.timeFormatter = resultSetSerializable.getTimeFormatter();
    this.timeZone = resultSetSerializable.getTimeZone();
    this.honorClientTZForTimestampNTZ = resultSetSerializable.isHonorClientTZForTimestampNTZ();
    this.binaryFormatter = resultSetSerializable.getBinaryFormatter();
    this.resultSetMetaData = resultSetSerializable.getSFResultSetMetaData();
    this.treatNTZAsUTC = resultSetSerializable.getTreatNTZAsUTC();
    this.formatDateWithTimezone = resultSetSerializable.getFormatDateWithTimeZone();
    this.useSessionTimezone = resultSetSerializable.getUseSessionTimezone();

    // sort result set if needed
    String rowsetBase64 = resultSetSerializable.getFirstChunkStringData();
    if (rowsetBase64 == null || rowsetBase64.isEmpty()) {
      this.currentChunkIterator = ArrowResultChunk.getEmptyChunkIterator();
    } else {
      if (sortResult) {
        // we don't support sort result when there are offline chunks
        if (resultSetSerializable.getChunkFileCount() > 0) {
          throw new SnowflakeSQLLoggedException(
              session,
              ErrorCode.CLIENT_SIDE_SORTING_NOT_SUPPORTED.getMessageCode(),
              SqlState.FEATURE_NOT_SUPPORTED);
        }

        this.currentChunkIterator =
            getSortedFirstResultChunk(resultSetSerializable.getFirstChunkStringData())
                .getIterator(this);
      } else {
        this.currentChunkIterator =
            buildFirstChunk(resultSetSerializable.getFirstChunkStringData()).getIterator(this);
      }
    }
  }

  private boolean fetchNextRow() throws SnowflakeSQLException {
    if (sortResult) {
      return fetchNextRowSorted();
    } else {
      return fetchNextRowUnsorted();
    }
  }

  /**
   * Goto next row. If end of current chunk, update currentChunkIterator to the beginning of next
   * chunk, if any chunk not being consumed yet.
   *
   * @return true if still have rows otherwise false
   */
  private boolean fetchNextRowUnsorted() throws SnowflakeSQLException {
    boolean hasNext = currentChunkIterator.next();

    if (hasNext) {
      return true;
    } else {
      if (nextChunkIndex < chunkCount) {
        try {
          eventHandler.triggerStateTransition(
              BasicEvent.QueryState.CONSUMING_RESULT,
              String.format(
                  BasicEvent.QueryState.CONSUMING_RESULT.getArgString(), queryId, nextChunkIndex));

          ArrowResultChunk nextChunk = (ArrowResultChunk) chunkDownloader.getNextChunkToConsume();

          if (nextChunk == null) {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Expect chunk but got null for chunk index " + nextChunkIndex);
          }

          currentChunkIterator.getChunk().freeData();
          currentChunkIterator = nextChunk.getIterator(this);
          if (currentChunkIterator.next()) {

            logger.debug(
                "Moving to chunk index {}, row count={}", nextChunkIndex, nextChunk.getRowCount());

            nextChunkIndex++;
            return true;
          } else {
            return false;
          }
        } catch (InterruptedException ex) {
          throw new SnowflakeSQLLoggedException(
              session, ErrorCode.INTERRUPTED.getMessageCode(), SqlState.QUERY_CANCELED);
        }
      } else {
        // always free current chunk
        try {
          currentChunkIterator.getChunk().freeData();
          if (chunkCount > 0) {
            logger.debug("End of chunks");
            DownloaderMetrics metrics = chunkDownloader.terminate();
            logChunkDownloaderMetrics(metrics);
          }
        } catch (InterruptedException e) {
          throw new SnowflakeSQLLoggedException(
              session, ErrorCode.INTERRUPTED.getMessageCode(), SqlState.QUERY_CANCELED);
        }
      }

      return false;
    }
  }

  /**
   * Decode rowset returned in query response the load data into arrow vectors
   *
   * @param rowsetBase64 first chunk of rowset in arrow format and base64 encoded
   * @return result chunk with arrow data already being loaded
   */
  private ArrowResultChunk buildFirstChunk(String rowsetBase64) throws SQLException {
    byte[] bytes = Base64.getDecoder().decode(rowsetBase64);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

    // create a result chunk
    ArrowResultChunk resultChunk = new ArrowResultChunk("", 0, 0, 0, rootAllocator, session);

    try {
      resultChunk.readArrowStream(inputStream);
    } catch (IOException e) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR,
          "Failed to " + "load data in first chunk into arrow vector ex: " + e.getMessage());
    }

    return resultChunk;
  }

  /**
   * Decode rowset returned in query response the load data into arrow vectors and sort data
   *
   * @param rowsetBase64 first chunk of rowset in arrow format and base64 encoded
   * @return result chunk with arrow data already being loaded
   */
  private ArrowResultChunk getSortedFirstResultChunk(String rowsetBase64) throws SQLException {
    ArrowResultChunk resultChunk = buildFirstChunk(rowsetBase64);

    // enable sorted chunk, the sorting happens when the result chunk is ready to consume
    resultChunk.enableSortFirstResultChunk();
    return resultChunk;
  }

  /**
   * Fetch next row of first chunkd in sorted order. If the result set huge, then rest of the chunks
   * are ignored.
   */
  private boolean fetchNextRowSorted() throws SnowflakeSQLException {
    boolean hasNext = currentChunkIterator.next();
    if (hasNext) {
      return true;
    } else {
      currentChunkIterator.getChunk().freeData();

      // no more chunks as sorted is only supported
      // for one chunk
      return false;
    }
  }

  /**
   * Advance to next row
   *
   * @return true if next row exists, false otherwise
   */
  @Override
  public boolean next() throws SFException, SnowflakeSQLException {
    if (isClosed()) {
      return false;
    }

    // otherwise try to fetch again
    if (fetchNextRow()) {
      row++;
      if (isLast()) {
        long timeConsumeLastResult = System.currentTimeMillis() - this.firstChunkTime;
        logMetric(TelemetryField.TIME_CONSUME_LAST_RESULT, timeConsumeLastResult);
      }
      return true;
    } else {
      logger.debug("end of result");

      /*
       * Here we check if the result has been truncated and throw exception if
       * so.
       */
      if (totalRowCountTruncated
          || Boolean.TRUE
              .toString()
              .equalsIgnoreCase(systemGetProperty("snowflake.enable_incident_test2"))) {
        throw (SFException)
            IncidentUtil.generateIncidentV2WithException(
                session, new SFException(ErrorCode.MAX_RESULT_LIMIT_EXCEEDED), queryId, null);
      }

      // mark end of result
      return false;
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toByte(index);
  }

  @Override
  public String getString(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toString(index);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toBoolean(index);
  }

  @Override
  public short getShort(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toShort(index);
  }

  @Override
  public int getInt(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toInt(index);
  }

  @Override
  public long getLong(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toLong(index);
  }

  @Override
  public float getFloat(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toFloat(index);
  }

  @Override
  public double getDouble(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toDouble(index);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toBytes(index);
  }

  @Override
  public Date getDate(int columnIndex, TimeZone tz) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    converter.setSessionTimeZone(timeZone);
    converter.setUseSessionTimezone(useSessionTimezone);
    return converter.toDate(index, tz, resultSetSerializable.getFormatDateWithTimeZone());
  }

  @Override
  public Time getTime(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    converter.setSessionTimeZone(timeZone);
    converter.setUseSessionTimezone(useSessionTimezone);
    return converter.toTime(index);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    converter.setSessionTimeZone(timeZone);
    converter.setUseSessionTimezone(useSessionTimezone);
    wasNull = converter.isNull(index);
    return converter.toTimestamp(index, tz);
  }

  @Override
  public Object getObject(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    converter.setTreatNTZAsUTC(treatNTZAsUTC);
    converter.setUseSessionTimezone(useSessionTimezone);
    converter.setSessionTimeZone(timeZone);
    return converter.toObject(index);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SFException {
    ArrowVectorConverter converter = currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    converter.setSessionTimeZone(timeZone);
    converter.setUseSessionTimezone(useSessionTimezone);
    return converter.toBigDecimal(index);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SFException {
    return getBigDecimal(columnIndex).setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public boolean isLast() {
    return nextChunkIndex == chunkCount && currentChunkIterator.isLast();
  }

  @Override
  public boolean isAfterLast() {
    return nextChunkIndex == chunkCount && currentChunkIterator.isAfterLast();
  }

  @Override
  public void close() throws SnowflakeSQLException {
    super.close();

    // always make sure to free this current chunk
    currentChunkIterator.getChunk().freeData();

    try {
      if (chunkDownloader != null) {
        DownloaderMetrics metrics = chunkDownloader.terminate();
        logChunkDownloaderMetrics(metrics);
      } else {
        // always close root allocator
        closeRootAllocator(rootAllocator);
      }
    } catch (InterruptedException ex) {
      throw new SnowflakeSQLLoggedException(
          session, ErrorCode.INTERRUPTED.getMessageCode(), SqlState.QUERY_CANCELED);
    }
  }

  public static void closeRootAllocator(RootAllocator rootAllocator) {
    long rest = rootAllocator.getAllocatedMemory();
    int count = 3;
    try {
      while (rest > 0 && count-- > 0) {
        // this case should only happen when the resultSet is closed before consuming all chunks
        // otherwise, the memory usage for each chunk will be cleared right after it has been fully
        // consumed

        // The reason is that it is possible that one downloading thread is pending to close when
        // the main thread
        // reaches here. A retry is to wait for the downloading thread to finish closing incoming
        // streams and arrow
        // resources.

        Thread.sleep(10);
        rest = rootAllocator.getAllocatedMemory();
      }
      if (rest == 0) {
        rootAllocator.close();
      }
    } catch (InterruptedException ie) {
      logger.debug("interrupted during closing root allocator");
    } catch (Exception e) {
      logger.debug("Exception happened when closing rootAllocator: ", e.getLocalizedMessage());
    }
  }

  @Override
  public SFStatementType getStatementType() {
    return statementType;
  }

  @Override
  public void setStatementType(SFStatementType statementType) {
    this.statementType = statementType;
  }

  @Override
  public boolean isArrayBindSupported() {
    return this.arrayBindSupported;
  }

  @Override
  public String getQueryId() {
    return queryId;
  }

  private void logMetric(TelemetryField field, long value) {
    TelemetryData data = TelemetryUtil.buildJobData(this.queryId, field, value);
    this.telemetryClient.addLogToBatch(data);
  }

  private void logChunkDownloaderMetrics(DownloaderMetrics metrics) {
    if (metrics != null) {
      logMetric(TelemetryField.TIME_WAITING_FOR_CHUNKS, metrics.getMillisWaiting());
      logMetric(TelemetryField.TIME_DOWNLOADING_CHUNKS, metrics.getMillisDownloading());
      logMetric(TelemetryField.TIME_PARSING_CHUNKS, metrics.getMillisParsing());
    }
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampLTZFormatter() {
    return timestampLTZFormatter;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampNTZFormatter() {
    return timestampNTZFormatter;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampTZFormatter() {
    return timestampTZFormatter;
  }

  @Override
  public SnowflakeDateTimeFormat getDateFormatter() {
    return dateFormatter;
  }

  @Override
  public SnowflakeDateTimeFormat getTimeFormatter() {
    return timeFormatter;
  }

  @Override
  public SFBinaryFormat getBinaryFormatter() {
    return binaryFormatter;
  }

  @Override
  public int getScale(int columnIndex) {
    return resultSetMetaData.getScale(columnIndex);
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public boolean getHonorClientTZForTimestampNTZ() {
    return honorClientTZForTimestampNTZ;
  }

  @Override
  public long getResultVersion() {
    return resultVersion;
  }
}
