package net.snowflake.client.internal.jdbc.cloud.storage;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.util.SFPair;

interface GCSAccessStrategy {
  StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix);

  StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix);

  Map<String, String> download(
      int parallelism, String remoteStorageLocation, String stageFilePath, File localFile)
      throws InterruptedException;

  SFPair<InputStream, Map<String, String>> downloadToStream(
      String remoteStorageLocation, String stageFilePath, boolean isEncrypting);

  void uploadWithDownScopedToken(
      int parallelism,
      String remoteStorageLocation,
      String destFileName,
      String contentEncoding,
      Map<String, String> metadata,
      long contentLength,
      InputStream content,
      String queryId)
      throws InterruptedException;

  boolean handleStorageException(
      Exception ex,
      int retryCount,
      String operation,
      SFSession session,
      String command,
      String queryId,
      SnowflakeGCSClient gcsClient)
      throws SnowflakeSQLException;

  void shutdown();
}
