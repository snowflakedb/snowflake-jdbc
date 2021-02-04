/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * Base class for file transfers: given a SnowflakeConnection, files may be uploaded or downloaded
 * from/to an InputStream.
 *
 * <p>Note that while SnowflakeFileTransferAgent is used liberally throughout the codebase for
 * performing uploads, the "alternative" implementations may have quite different ways of uploading
 * to cloud storage, so this is a rather "thin" abstract class that leaves much of the
 * implementation up to the implementing class.
 *
 * <p>It is also expected that a command (GET/PUT) is parsed by the FileTransferAgent before either
 * execute() or downloadStream() is called. This is not enforced by the abstract class's interface,
 * but will be passed to the SFConnectionHandler's getFileTransferAgent().
 */
public abstract class SFBaseFileTransferAgent implements SnowflakeFixedView {
  protected boolean compressSourceFromStream;
  protected String destFileNameForStreamSource;
  protected InputStream sourceStream;
  protected boolean sourceFromStream;

  /**
   * Sets the source data stream to be uploaded.
   *
   * @param sourceStream The source data to upload.
   */
  public void setSourceStream(InputStream sourceStream) {
    this.sourceStream = sourceStream;
    this.sourceFromStream = true;
  }

  /**
   * Sets the target filename for uploading
   *
   * @param destFileNameForStreamSource The target destination filename once the file is uploaded.
   */
  public void setDestFileNameForStreamSource(String destFileNameForStreamSource) {
    this.destFileNameForStreamSource = destFileNameForStreamSource;
  }

  /**
   * Whether to compress the source stream before upload.
   *
   * @param compressSourceFromStream boolean for whether to compress the data stream before upload.
   */
  public void setCompressSourceFromStream(boolean compressSourceFromStream) {
    this.compressSourceFromStream = compressSourceFromStream;
  }

  /**
   * Run the PUT/GET command, if a command has been set.
   *
   * @return Whether the operation was completed successfully, and completely.
   * @throws SQLException for SQL or upload errors
   */
  public abstract boolean execute() throws SQLException;

  /**
   * Download data from a stage.
   *
   * @param fileName A file on a stage to download.
   * @return An InputStream for the requested file.
   * @throws SnowflakeSQLException If the file does not exist, or if an error occurred during
   *     transport.
   */
  public abstract InputStream downloadStream(String fileName) throws SnowflakeSQLException;

  /** The types of file transfer: upload and download. */
  public enum CommandType {
    UPLOAD,
    DOWNLOAD
  }
}
