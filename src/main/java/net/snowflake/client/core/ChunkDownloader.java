/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.SnowflakeResultChunk;
import net.snowflake.client.jdbc.SnowflakeSQLException;

/**
 * Provide offline result chunk (which contains result data) to back to result
 * set
 */
public interface ChunkDownloader
{
  /**
   * Get next SnowflakeResultChunk that is ready to be consumed by the main
   * thread. The caller will be blocked if the chunk is not ready to be
   * consumed (a.k.a not loaded into memory yet)
   *
   * @return result chunk with data loaded
   */
  SnowflakeResultChunk getNextChunkToConsume()
  throws InterruptedException, SnowflakeSQLException;

  /**
   * Terminate the chunk downloader, release all resources allocated
   *
   * @return metrics measuring downloader performance
   */
  DownloaderMetrics terminate() throws InterruptedException;
}
