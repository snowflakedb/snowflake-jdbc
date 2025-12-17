package net.snowflake.client.api.resultset;

import java.sql.SQLException;

/** This interface defines Snowflake specific APIs for asynchronous ResultSet */
public interface SnowflakeAsyncResultSet extends SnowflakeResultSet {
  /**
   * This function retrieves the status of an asynchronous query. An empty ResultSet object has
   * already been returned, but the query may still be running. This function can be used to query
   * whether it is possible to retrieve results from the ResultSet already.
   *
   * <p><code>status.isSuccess()</code> means that results can be retrieved.
   *
   * @return an instance containing query metadata
   * @throws SQLException if an error is encountered
   */
  QueryStatus getStatus() throws SQLException;
}
