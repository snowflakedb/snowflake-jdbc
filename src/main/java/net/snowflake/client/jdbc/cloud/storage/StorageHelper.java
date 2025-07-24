package net.snowflake.client.jdbc.cloud.storage;

import java.io.File;
import java.io.InputStream;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class StorageHelper {
  private static final SFLogger logger = SFLoggerFactory.getLogger(StorageHelper.class);

  protected static final String DOWNLOAD = "download";
  protected static final String UPLOAD = "upload";

  static String getStartUploadLog(
      String serviceName,
      boolean uploadFromStream,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      File srcFile,
      String destFileName) {
    if (uploadFromStream && fileBackedOutputStream != null) {
      File file = fileBackedOutputStream.getFile();
      String fileBackedOutputStreamType =
          file == null ? "byte stream" : ("file: " + file.getAbsolutePath());
      return "Starting upload from stream ("
          + fileBackedOutputStreamType
          + ") to "
          + serviceName
          + " location: "
          + destFileName;
    } else if (uploadFromStream && inputStream != null) {
      return "Starting upload from input stream to " + serviceName + " location: " + destFileName;
    } else {
      return "Starting upload from file "
          + srcFile.getAbsolutePath()
          + " to "
          + serviceName
          + " location: "
          + destFileName;
    }
  }

  static ErrorCode getOperationException(String operation) {
    switch (operation) {
      case UPLOAD:
        return ErrorCode.UPLOAD_ERROR;
      case DOWNLOAD:
        return ErrorCode.DOWNLOAD_ERROR;
      default:
        logger.warn(
            "Unknown operation: {}. Returning fallback error code: ErrorCode.FILE_TRANSFER_ERROR",
            operation);
        return ErrorCode.FILE_TRANSFER_ERROR;
    }
  }
}
