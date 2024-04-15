package net.snowflake.client.jdbc.cloud.storage;

import java.io.File;
import java.io.InputStream;
import net.snowflake.client.jdbc.FileBackedOutputStream;

class StorageHelper {
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
}
