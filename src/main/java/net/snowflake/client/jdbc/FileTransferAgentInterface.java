/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.sql.SQLException;

public interface FileTransferAgentInterface extends SnowflakeFixedView {
  void setSourceStream(InputStream sourceStream);

  void setDestFileNameForStreamSource(String destFileNameForStreamSource);

  void setCompressSourceFromStream(boolean compressSourceFromStream);

  boolean execute() throws SQLException;

  InputStream downloadStream(String fileName) throws SnowflakeSQLException;
}
