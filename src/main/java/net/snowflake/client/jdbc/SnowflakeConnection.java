/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * This interface defines Snowflake specific APIs for Connection
 */
public interface SnowflakeConnection
{
  /**
   * Method to compress data from a stream and upload it at a stage location.
   * The data will be uploaded as one file. No splitting is done in this method.
   * <p>
   * caller is responsible for releasing the inputStream after the method is
   * called.
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param compressData compress data or not before uploading stream
   * @throws SQLException failed to compress and put data from a stream at stage
   */
  void uploadStream(String stageName,
                    String destPrefix,
                    InputStream inputStream,
                    String destFileName,
                    boolean compressData) throws SQLException;
}
