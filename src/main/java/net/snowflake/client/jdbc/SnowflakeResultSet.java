/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.core.structs.SFSqlData;

/** This interface defines Snowflake specific APIs for ResultSet */
public interface SnowflakeResultSet {

  <T extends SFSqlData> List<T> getList(int columnIndex, Class<T> type) throws SQLException;

  <T extends SFSqlData> T[] getArray(int columnIndex, Class<T> type) throws SQLException;

  <K, T extends SFSqlData> Map<K, T> getMap(int columnIndex, Class<K> keyType, Class<T> type) throws SQLException;

  /**
   * @return the Snowflake query ID of the query which generated this result set
   */
  String getQueryID() throws SQLException;

  /**
   * This function retrieves the status of an asynchronous query. An empty ResultSet object has
   * already been returned but the query may still be running. This function can be used to poll to
   * see if it is possible to retrieve results from the ResultSet yet. See
   * Client/src/main/java/net/snowflake/client/core/QueryStatus.java for the list of all possible
   * query statuses. QueryStatus = SUCCESS means results can be retrieved.
   *
   * @return QueryStatus enum showing status of query
   * @throws SQLException
   */
  QueryStatus getStatus() throws SQLException;

  /**
   * This function retrieves the error message recorded from the error status of an asynchronous
   * query. If there is no error or no error is returned by the server, an empty string will be
   * returned.
   *
   * @return String value of query's error message
   * @throws SQLException
   */
  String getQueryErrorMessage() throws SQLException;

  /**
   * Get a list of ResultSetSerializables for the ResultSet in order to parallel processing
   *
   * @param maxSizeInBytes The expected max data size wrapped in the ResultSetSerializables object.
   *     NOTE: this parameter is intended to make the data size in each serializable object to be
   *     less than it. But if user specifies a small value which may be smaller than the data size
   *     of one result chunk. So the definition can't be guaranteed completely. For this special
   *     case, one serializable object is used to wrap the data chunk.
   * @return a list of ResultSetSerializables
   * @throws SQLException if fails to get the ResultSetSerializable objects.
   */
  List<SnowflakeResultSetSerializable> getResultSetSerializables(long maxSizeInBytes)
      throws SQLException;
}
