package net.snowflake.client.jdbc;

public interface SnowflakeFileTransferMetadata {
  /**
   * Determine this metadata is for transferring one or multiple files.
   *
   * @return return true if it is for transferring one file.
   */
  boolean isForOneFile();
}
