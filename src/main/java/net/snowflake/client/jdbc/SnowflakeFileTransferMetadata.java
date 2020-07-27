/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

public interface SnowflakeFileTransferMetadata {
  /**
   * Determine this metadata is for transferring one or multiple files.
   *
   * @return return true if it is for transferring one file.
   */
  boolean isForOneFile();
}
