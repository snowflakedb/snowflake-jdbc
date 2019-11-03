/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.util.List;
import java.sql.SQLException;

/**
 * This interface defines Snowflake specific APIs for ResultSet
 */
public interface SnowflakeResultSet
{
  /**
   * @return the Snowflake query ID of the query which generated this result set
   */
  String getQueryID() throws SQLException;

  /**
   * Get a list of ResultSetSerializables for the ResultSet in order to parallel processing
   *
   * @param maxSizeInBytes The expected max data size wrapped in the
   *                       ResultSetSerializables object.
   *                       NOTE: this parameter is intended to make the data
   *                       size in each serializable object to be less than it.
   *                       But if user specifies a small value which may be
   *                       smaller than the data size of one result chunk.
   *                       So the definition can't be guaranteed completely.
   *                       For this special case, one serializable object is
   *                       used to wrap the data chunk.
   * @return a list of ResultSetSerializables
   * @throws SQLException if fails to get the ResultSetSerializable objects.
   */
  List<SnowflakeResultSetSerializable> getResultSetSerializables(long maxSizeInBytes) throws SQLException;
}
