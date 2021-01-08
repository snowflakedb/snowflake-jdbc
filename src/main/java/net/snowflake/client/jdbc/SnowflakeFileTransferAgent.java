/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;

/**
 * Class for uploading/downloading files
 *
 * @author jhuang
 */
public interface SnowflakeFileTransferAgent extends SnowflakeFixedView {
  void setSourceStream(InputStream sourceStream);

  void setDestFileNameForStreamSource(String destFileNameForStreamSource);

  void setCompressSourceFromStream(boolean compressSourceFromStream);

  void execute();

  enum CommandType {
    UPLOAD,
    DOWNLOAD
  }
}
