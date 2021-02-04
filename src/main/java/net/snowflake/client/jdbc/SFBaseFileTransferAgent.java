/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.sql.SQLException;

public abstract class SFBaseFileTransferAgent implements SnowflakeFixedView {
  protected boolean compressSourceFromStream;
  protected String destFileNameForStreamSource;
  protected InputStream sourceStream;
  protected boolean sourceFromStream;

  public void setSourceStream(InputStream sourceStream) {
    this.sourceStream = sourceStream;
    this.sourceFromStream = true;
  }

  public void setDestFileNameForStreamSource(String destFileNameForStreamSource) {
    this.destFileNameForStreamSource = destFileNameForStreamSource;
  }

  public void setCompressSourceFromStream(boolean compressSourceFromStream) {
    this.compressSourceFromStream = compressSourceFromStream;
  }

  public abstract boolean execute() throws SQLException;

  public abstract InputStream downloadStream(String fileName) throws SnowflakeSQLException;

  public enum CommandType {
    UPLOAD,
    DOWNLOAD
  }
}
