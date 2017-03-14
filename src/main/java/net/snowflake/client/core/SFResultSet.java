/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.common.core.SqlState;
import net.snowflake.client.core.BasicEvent.QueryState;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeChunkDownloader;
import net.snowflake.client.jdbc.SnowflakeResultChunk;
import net.snowflake.client.jdbc.SnowflakeSQLException;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import static net.snowflake.client.core.StmtUtil.eventHandler;

/**
 * Snowflake ResultSet implementation
 * <p>
 * @author jhuang
 */
public class SFResultSet extends SFBaseResultSet
{
  static final SFLogger logger = SFLoggerFactory.getLogger(SFResultSet.class);

  private int columnCount = 0;

  private int currentChunkRowCount = 0;

  private int currentChunkRowIndex = -1;

  private JsonNode firstChunkRowset = null;

  private SnowflakeResultChunk currentChunk = null;

  private String queryId;

  private long statementTypeId;

  private boolean totalRowCountTruncated;

  private boolean sortResult = false;

  private Object[][] firstChunkSortedRowSet;

  private long chunkCount = 0;

  private long nextChunkIndex = 0;

  private SnowflakeChunkDownloader chunkDownloader;

  protected SFStatement statement;

  /**
   * Constructor takes a result from the API response that we get from
   * executing a SQL statement.
   * <p>
   * The constructor will initialize the ResultSetMetaData.
   * <p>
   * @param result
   * @param statement
   * @param sortResult
   */
  public SFResultSet(JsonNode result,
                     SFStatement statement,
                     boolean sortResult)
      throws SQLException, SFException
  {
    this.statement = statement;
    this.columnCount = 0;
    this.sortResult = sortResult;

    SFSession session = this.statement.getSession();

    ResultUtil.ResultInput resultInput = new ResultUtil.ResultInput();
    resultInput.setResultJSON(result)
        .setConnectionTimeout(session.getHttpClientConnectionTimeout())
        .setSocketTimeout(session.getHttpClientSocketTimeout())
        .setNetworkTimeoutInMilli(session.getNetworkTimeoutInMilli())
        .setUseProxy(session.isUseProxy());

    ResultUtil.ResultOutput resultOutput = ResultUtil.processResult(resultInput);

    this.queryId = resultOutput.getQueryId();
    this.statementTypeId = resultOutput.getStatementTypeId();
    this.totalRowCountTruncated = resultOutput.isTotalRowCountTruncated();
    this.parameters = resultOutput.getParameters();
    this.columnCount = resultOutput.getColumnCount();
    this.firstChunkRowset = resultOutput.getAndClearCurrentChunkRowset();
    this.currentChunkRowCount = resultOutput.getCurrentChunkRowCount();
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
    this.isClosed = false;

    session.setDatabase(resultOutput.getFinalDatabaseName());
    session.setSchema(resultOutput.getFinalSchemaName());
    // update the driver/session with common parameters from GS
    SessionUtil.updateSfDriverParamValues(this.parameters, statement.getSession());

    // sort result set if needed
    if (sortResult)
    {
      // we don't support sort result when there are offline chunks
      if (chunkCount > 0)
      {
        throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
                                        ErrorCode.CLIENT_SIDE_SORTING_NOT_SUPPORTED.getMessageCode());
      }

      sortResultSet();
    }

    eventHandler.triggerStateTransition(BasicEvent.QueryState.CONSUMING_RESULT,
        String.format(QueryState.CONSUMING_RESULT.getArgString(), queryId, 0));

    resultSetMetaData = new SFResultSetMetaData(resultOutput.getResultColumnMetadata(),
                                                queryId,
                                                session,
                                                this.timestampNTZFormatter,
                                                this.timestampLTZFormatter,
                                                this.timestampTZFormatter,
                                                this.dateFormatter,
                                                this.timeFormatter);
  }

  private boolean fetchNextRow() throws SFException, SnowflakeSQLException
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

  private boolean fetchNextRowSorted()
  {
    currentChunkRowIndex++;

    if (currentChunkRowIndex < currentChunkRowCount)
    {
      return true;
    }

    firstChunkSortedRowSet = null;

    // no more chunks as sorted is only supported
    // for one chunk
    return false;
  }

  private boolean fetchNextRowUnsorted() throws SFException, SnowflakeSQLException
  {
    logger.debug("Entering fetchJSONNextRow");

    currentChunkRowIndex++;

    if (currentChunkRowIndex < currentChunkRowCount)
    {
      return true;
    }

    // let GC collect first rowset
    firstChunkRowset = null;

    if (nextChunkIndex < chunkCount)
    {
      try
      {
        eventHandler.triggerStateTransition(
            BasicEvent.QueryState.CONSUMING_RESULT,
            String.format(QueryState.CONSUMING_RESULT.getArgString(),
                          queryId,
                          nextChunkIndex));

        SnowflakeResultChunk nextChunk = chunkDownloader.getNextChunkToConsume();

        if (nextChunk == null)
          throw new SnowflakeSQLException(
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "Expect chunk but got null for chunk index " + nextChunkIndex);

        currentChunkRowIndex = 0;
        currentChunkRowCount = nextChunk.getRowCount();
        currentChunk = nextChunk;

        logger.info("Moving to chunk index {}, row count={}",
                   nextChunkIndex, currentChunkRowCount);

        nextChunkIndex++;

        return true;
      } catch (InterruptedException ex)
      {
        throw new SnowflakeSQLException(SqlState.QUERY_CANCELED,
                                        ErrorCode.INTERRUPTED.getMessageCode());
      }
    }
    else if (chunkCount > 0)
    {
      logger.info("End of chunks");
      chunkDownloader.terminate();
    }

    return false;
  }

  /**
   * Advance to next row
   * <p>
   * @return true if next row exists, false otherwise
   * <p>
   */
  @Override
  public boolean next() throws SFException, SnowflakeSQLException
  {
    if (isClosed())
    {
      throw new SFException(ErrorCode.RESULTSET_ALREADY_CLOSED);
    }

    // otherwise try to fetch again
    if (fetchNextRow())
    {
      row++;
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
        throw IncidentUtil.
            generateIncidentWithException(session, null, queryId,
                                          ErrorCode.MAX_RESULT_LIMIT_EXCEEDED);
      }

      // mark end of result
      return false;
    }
  }

  @Override
  protected Object getObjectInternal(int columnIndex) throws SFException
  {
    if (columnIndex <= 0 || columnIndex > resultSetMetaData.getColumnCount())
    {
      throw new SFException(ErrorCode.COLUMN_DOES_NOT_EXIST, columnIndex);
    }

    final int internalColumnIndex = columnIndex - 1;
    if (sortResult)
    {
      return firstChunkSortedRowSet[currentChunkRowIndex][internalColumnIndex];
    }
    else if (firstChunkRowset != null)
    {
      return SnowflakeResultChunk.extractCell(firstChunkRowset,
                                              currentChunkRowIndex,
                                              internalColumnIndex);
    }
    else
    {
      return currentChunk.getCell(currentChunkRowIndex, internalColumnIndex);
    }
  }

  private void sortResultSet() throws SnowflakeSQLException, SFException
  {
    // first fetch rows into firstChunkSortedRowSet
    firstChunkSortedRowSet = new Object[currentChunkRowCount][];

    for (int rowIdx = 0; rowIdx < currentChunkRowCount; rowIdx++)
    {
      firstChunkSortedRowSet[rowIdx] = new Object[columnCount];
      for (int colIdx = 0; colIdx < columnCount; colIdx++)
      {
        firstChunkSortedRowSet[rowIdx][colIdx] =
            SnowflakeResultChunk.extractCell(firstChunkRowset,
                                             rowIdx, colIdx);
      }
    }

    // now sort it
    Arrays.sort(firstChunkSortedRowSet,
                new Comparator<Object[]>()
                {
                  public int compare(Object[] a, Object[] b)
                  {
                    int numCols = a.length;

                    for (int colIdx = 0; colIdx < numCols; colIdx++)
                    {
                      if (a[colIdx] == null && b[colIdx] == null)
                      {
                        continue;
                      }

                      // null is considered bigger than all values
                      if (a[colIdx] == null)
                      {
                        return 1;
                      }

                      if (b[colIdx] == null)
                      {
                        return -1;
                      }

                      int res
                          = a[colIdx].toString().compareTo(b[colIdx].toString());

                      // continue to next column if no difference
                      if (res == 0)
                      {
                        continue;
                      }

                      return res;
                    }

                    // all columns are the same
                    return 0;
                  }
                });
  }

  @Override
  public void close()
  {
    super.close();

    if (chunkDownloader != null)
    {
      chunkDownloader.terminate();
      firstChunkSortedRowSet = null;
      firstChunkRowset = null;
      currentChunk = null;
    }
  }

  public long getStatementTypeId()
  {
    return this.statementTypeId;
  }
}
