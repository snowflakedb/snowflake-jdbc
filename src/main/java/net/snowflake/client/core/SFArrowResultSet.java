/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.ArrowResultChunk;
import net.snowflake.client.jdbc.ArrowResultChunk.ArrowChunkIterator;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.TimeZone;


import static net.snowflake.client.core.StmtUtil.eventHandler;

/**
 * Arrow result set implementation
 */
public class SFArrowResultSet extends SFBaseResultSet
{
  static final SFLogger logger =
      SFLoggerFactory.getLogger(SFArrowResultSet.class);

  /**
   * iterator over current arrow result chunk
   */
  private ArrowChunkIterator currentChunkIterator;

  /**
   * current query id
   */
  private String queryId;

  /**
   * type of statement generate this result set
   */
  private SFStatementType statementType;

  private boolean totalRowCountTruncated;

  /**
   * true if sort first chunk
   */
  private boolean sortResult;

  /**
   * statement generate current result set
   */
  protected SFStatement statement;

  /**
   * is array bind supported
   */
  private final boolean arrayBindSupported;

  /**
   * sesion timezone
   */
  private TimeZone timeZone;

  /**
   * index of next chunk to consume
   */
  private long nextChunkIndex = 0;

  /**
   * total chunk count, not include first chunk
   */
  private final long chunkCount;

  /**
   * chunk downloader
   */
  private ChunkDownloader chunkDownloader;

  /**
   * time when first chunk arrived
   */
  private final long firstChunkTime;

  /**
   * telemetry client to push stats to server
   */
  private final Telemetry telemetryClient;

  /**
   * Constructor takes a result from the API response that we get from
   * executing a SQL statement.
   * <p>
   * The constructor will initialize the ResultSetMetaData.
   *
   * @param resultOutput result data after parsing json
   * @param statement    statement object
   * @param sortResult   true if sort results otherwise false
   * @throws SQLException exception raised from general SQL layers
   */
  SFArrowResultSet(ResultUtil.ResultOutput resultOutput,
                   SFStatement statement,
                   boolean sortResult)
  throws SQLException
  {
    this(resultOutput, statement.getSession().getTelemetryClient(), sortResult);

    // update the session db/schema/wh/role etc
    this.statement = statement;
    SFSession session = this.statement.getSession();
    session.setDatabase(resultOutput.getFinalDatabaseName());
    session.setSchema(resultOutput.getFinalSchemaName());
    session.setRole(resultOutput.getFinalRoleName());
    session.setWarehouse(resultOutput.getFinalWarehouseName());

    // update the driver/session with common parameters from GS
    SessionUtil
        .updateSfDriverParamValues(this.parameters, statement.getSession());

    // if server gives a send time, log time it took to arrive
    if (resultOutput.getSendResultTime() != 0)
    {
      long timeConsumeFirstResult = this.firstChunkTime - resultOutput.getSendResultTime();
      logMetric(TelemetryField.TIME_CONSUME_FIRST_RESULT, timeConsumeFirstResult);
    }

    eventHandler.triggerStateTransition(BasicEvent.QueryState.CONSUMING_RESULT,
                                        String.format(
                                            BasicEvent.QueryState.CONSUMING_RESULT
                                                .getArgString(), queryId, 0));


  }

  /**
   * This is a minimum initialization for SFArrowResult. Mainly used for testing
   * purpose. However, real prod constructor will call this constructor as well
   *
   * @param resultOutput    data returned in query response
   * @param telemetryClient telemetryClient
   * @throws SQLException
   */
  SFArrowResultSet(ResultUtil.ResultOutput resultOutput,
                   Telemetry telemetryClient,
                   boolean sortResult)
  throws SQLException
  {
    this.sortResult = sortResult;
    this.queryId = resultOutput.getQueryId();
    this.statementType = resultOutput.getStatementType();
    this.totalRowCountTruncated = resultOutput.isTotalRowCountTruncated();
    this.parameters = resultOutput.getParameters();
    this.chunkCount = resultOutput.getChunkCount();
    this.chunkDownloader = resultOutput.getChunkDownloader();
    this.timestampNTZFormatter = resultOutput.getTimestampNTZFormatter();
    this.timestampLTZFormatter = resultOutput.getTimestampLTZFormatter();
    this.timestampTZFormatter = resultOutput.getTimestampTZFormatter();
    this.dateFormatter = resultOutput.getDateFormatter();
    this.timeFormatter = resultOutput.getTimeFormatter();
    this.timeZone = resultOutput.getTimeZone();
    this.honorClientTZForTimestampNTZ =
        resultOutput.isHonorClientTZForTimestampNTZ();
    this.binaryFormatter = resultOutput.getBinaryFormatter();
    this.resultVersion = resultOutput.getResultVersion();
    this.numberOfBinds = resultOutput.getNumberOfBinds();
    this.arrayBindSupported = resultOutput.isArrayBindSupported();
    this.metaDataOfBinds = resultOutput.getMetaDataOfBinds();
    this.telemetryClient = telemetryClient;
    this.firstChunkTime = System.currentTimeMillis();

    // sort result set if needed
    String rowsetBase64 = resultOutput.getRowsetBase64();
    if (rowsetBase64 == null || rowsetBase64.isEmpty())
    {
      this.currentChunkIterator = ArrowResultChunk.getEmptyChunkIterator();
    }
    else
    {
      if (sortResult)
      {
        // we don't support sort result when there are offline chunks
        if (resultOutput.getChunkCount() > 0)
        {
          throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
                                          ErrorCode.CLIENT_SIDE_SORTING_NOT_SUPPORTED
                                              .getMessageCode());
        }

        this.currentChunkIterator = getSortedFirstResultChunk(
            resultOutput.getRowsetBase64()).getIterator();
      }
      else
      {
        this.currentChunkIterator =
            buildFirstChunk(resultOutput.getRowsetBase64()).getIterator();
      }
    }

    resultSetMetaData =
        new SFResultSetMetaData(resultOutput.getResultColumnMetadata(),
                                queryId,
                                session,
                                this.timestampNTZFormatter,
                                this.timestampLTZFormatter,
                                this.timestampTZFormatter,
                                this.dateFormatter,
                                this.timeFormatter);
  }

  private boolean fetchNextRow() throws SnowflakeSQLException
  {
    if (sortResult)
    {
      return fetchNextRowSorted();
    }
    else
    {
      return fetchNextRowUnsorted();
    }
  }

  /**
   * Goto next row. If end of current chunk, update currentChunkIterator to the
   * beginning of next chunk, if any chunk not being consumed yet.
   *
   * @return true if still have rows otherwise false
   */
  private boolean fetchNextRowUnsorted() throws SnowflakeSQLException
  {
    boolean hasNext = currentChunkIterator.next();

    if (hasNext)
    {
      return true;
    }
    else
    {
      if (nextChunkIndex < chunkCount)
      {
        try
        {
          eventHandler.triggerStateTransition(
              BasicEvent.QueryState.CONSUMING_RESULT,
              String.format(
                  BasicEvent.QueryState.CONSUMING_RESULT.getArgString(),
                  queryId,
                  nextChunkIndex));

          ArrowResultChunk
              nextChunk = (ArrowResultChunk) chunkDownloader.getNextChunkToConsume();

          if (nextChunk == null)
          {
            throw new SnowflakeSQLException(
                SqlState.INTERNAL_ERROR,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                "Expect chunk but got null for chunk index " + nextChunkIndex);
          }

          currentChunkIterator.getChunk().freeData();
          currentChunkIterator = nextChunk.getIterator();
          if (currentChunkIterator.next())
          {

            logger.debug("Moving to chunk index {}, row count={}",
                         nextChunkIndex, nextChunk.getRowCount());

            nextChunkIndex++;
            return true;
          }
          else
          {
            return false;
          }
        }
        catch (InterruptedException ex)
        {
          throw new SnowflakeSQLException(SqlState.QUERY_CANCELED,
                                          ErrorCode.INTERRUPTED.getMessageCode());
        }
      }
      else if (chunkCount > 0)
      {
        logger.debug("End of chunks");
        DownloaderMetrics metrics = chunkDownloader.terminate();
        logChunkDownloaderMetrics(metrics);
      }

      return false;

    }

  }

  /**
   * Decode rowset returned in query response the load data into arrow vectors
   *
   * @param rowsetBase64 first chunk of rowset in arrow format and base64
   *                     encoded
   * @return result chunk with arrow data already being loaded
   */
  private ArrowResultChunk buildFirstChunk(String rowsetBase64)
  throws SQLException
  {
    byte[] bytes = Base64.getDecoder().decode(rowsetBase64);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

    // create a fake result chunk
    ArrowResultChunk resultChunk = new ArrowResultChunk("", 0, 0, 0);

    try
    {
      ArrowResultChunk.readArrowStream(inputStream, resultChunk);
    }
    catch (IOException e)
    {
      throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR, "Failed to " +
                                                                "load data in first chunk into arrow vector ex: " +
                                                                e.getMessage());
    }

    return resultChunk;
  }

  /**
   * Decode rowset returned in query response the load data into arrow vectors
   * and sort data
   *
   * @param rowsetBase64 first chunk of rowset in arrow format and base64
   *                     encoded
   * @return result chunk with arrow data already being loaded
   */
  private ArrowResultChunk getSortedFirstResultChunk(String rowsetBase64)
  {
    //TODO
    return null;
  }


  /**
   * Fetch next row of first chunkd in sorted order. If the result set huge,
   * then rest of the chunks are ignored.
   */
  private boolean fetchNextRowSorted()
  {
    boolean hasNext = currentChunkIterator.next();
    if (hasNext)
    {
      return true;
    }
    else
    {
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
  public boolean next() throws SFException, SnowflakeSQLException
  {
    if (isClosed())
    {
      return false;
    }

    // otherwise try to fetch again
    if (fetchNextRow())
    {
      row++;
      if (isLast())
      {
        long timeConsumeLastResult = System.currentTimeMillis() - this.firstChunkTime;
        logMetric(TelemetryField.TIME_CONSUME_LAST_RESULT, timeConsumeLastResult);
      }
      return true;
    }
    else
    {
      logger.debug("end of result");

      /*
       * Here we check if the result has been truncated and throw exception if
       * so.
       */
      if (totalRowCountTruncated ||
          System.getProperty("snowflake.enable_incident_test2") != null &&
          System.getProperty("snowflake.enable_incident_test2").equals("true"))
      {
        throw (SFException) IncidentUtil.generateIncidentV2WithException(
            session,
            new SFException(ErrorCode.MAX_RESULT_LIMIT_EXCEEDED),
            queryId,
            null);
      }

      // mark end of result
      return false;
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toByte(index);
  }

  @Override
  public String getString(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toBoolean(index);
  }

  @Override
  public short getShort(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toShort(index);
  }

  @Override
  public int getInt(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toInt(index);
  }

  @Override
  public long getLong(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toLong(index);
  }

  @Override
  public float getFloat(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toFloat(index);
  }

  @Override
  public double getDouble(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toDouble(index);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toBytes(index);
  }

  @Override
  public Date getDate(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toDate(index);
  }

  @Override
  public Time getTime(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toTime(index);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, TimeZone tz)
  throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toTimestamp(index);
  }

  @Override
  public Object getObject(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toObject(index);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toBigDecimal(index);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SFException
  {
    ArrowVectorConverter converter =
        currentChunkIterator.getCurrentConverter(columnIndex - 1);
    int index = currentChunkIterator.getCurrentRowInRecordBatch();
    wasNull = converter.isNull(index);
    return converter.toBigDecimal(index, scale);
  }

  @Override
  public boolean isLast()
  {
    return nextChunkIndex == chunkCount &&
           currentChunkIterator.isLast();
  }

  @Override
  public boolean isAfterLast()
  {
    return nextChunkIndex == chunkCount &&
           currentChunkIterator.isAfterLast();
  }

  @Override
  public void close()
  {
    super.close();

    if (chunkDownloader != null)
    {
      DownloaderMetrics metrics = chunkDownloader.terminate();
      logChunkDownloaderMetrics(metrics);
    }
  }

  @Override
  public SFStatementType getStatementType()
  {
    return statementType;
  }

  @Override
  public void setStatementType(SFStatementType statementType)
  {
    this.statementType = statementType;
  }

  @Override
  public boolean isArrayBindSupported()
  {
    return this.arrayBindSupported;
  }

  @Override
  public String getQueryId()
  {
    return queryId;
  }

  private void logMetric(TelemetryField field, long value)
  {
    TelemetryData data = TelemetryUtil.buildJobData(this.queryId, field, value);
    this.telemetryClient.tryAddLogToBatch(data);
  }

  private void logChunkDownloaderMetrics(DownloaderMetrics metrics)
  {
    if (metrics != null)
    {
      logMetric(TelemetryField.TIME_WAITING_FOR_CHUNKS,
                metrics.getMillisWaiting());
      logMetric(TelemetryField.TIME_DOWNLOADING_CHUNKS,
                metrics.getMillisDownloading());
      logMetric(TelemetryField.TIME_PARSING_CHUNKS,
                metrics.getMillisParsing());
    }
  }
}
