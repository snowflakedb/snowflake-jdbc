/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This interface defines Snowflake specific APIs to access the data wrapped in the result set
 * serializable object.
 */
public interface SnowflakeResultSetSerializable {
  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * @return a ResultSet which represents for the data wrapped in the object
   */
  ResultSet getResultSet() throws SQLException;

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * @param info The proxy server information if proxy is necessary.
   * @return a ResultSet which represents for the data wrapped in the object
   */
  ResultSet getResultSet(Properties info) throws SQLException;

  /**
   * Retrieve total row count included in the the ResultSet Serializable object.
   *
   * @return the total row count from metadata
   */
  long getRowCount() throws SQLException;

  /**
   * Retrieve compressed data size included in the the ResultSet Serializable object.
   *
   * @return the total compressed data size in bytes from metadata
   */
  long getCompressedDataSizeInBytes() throws SQLException;

  /**
   * Retrieve uncompressed data size included in the the ResultSet Serializable object.
   *
   * @return the total uncompressed data size in bytes from metadata
   */
  long getUncompressedDataSizeInBytes() throws SQLException;
}
