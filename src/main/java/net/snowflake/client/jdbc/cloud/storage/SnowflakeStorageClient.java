/*
 * Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.*;
import net.snowflake.common.core.SqlState;

/**
 * Interface for storage client provider implementations
 *
 * @author lgiakoumakis
 */
public interface SnowflakeStorageClient {
  /** @return Returns the Max number of retry attempts */
  int getMaxRetries();

  /**
   * Returns the max exponent for multiplying backoff with the power of 2, the value of 4 will give
   * us 16secs as the max number of time to sleep before retry
   *
   * @return Returns the exponent
   */
  int getRetryBackoffMaxExponent();

  /** @return Returns the min number of milliseconds to sleep before retry */
  int getRetryBackoffMin();

  /** @return Returns true if encryption is enabled */
  boolean isEncrypting();

  /** @return Returns the size of the encryption key */
  int getEncryptionKeySize();

  /**
   * @return Whether this client requires the use of presigned URLs for upload and download instead
   *     of credentials that work for all files uploaded/ downloaded to a stage path. True for GCS.
   */
  default boolean requirePresignedUrl() {
    return false;
  }

  /**
   * Re-creates the encapsulated storage client with a fresh access token
   *
   * @param stageCredentials a Map (as returned by GS) which contains the new credential properties
   * @throws SnowflakeSQLException failure to renew the storage client
   */
  void renew(Map<?, ?> stageCredentials) throws SnowflakeSQLException;

  /** shuts down the client */
  void shutdown();

  /**
   * For a set of remote storage objects under a remote location and a given prefix/path returns
   * their properties wrapped in ObjectSummary objects
   *
   * @param remoteStorageLocation location, i.e. bucket for S3
   * @param prefix the prefix to list
   * @return a collection of storage summary objects
   * @throws StorageProviderException cloud storage provider error
   */
  StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix)
      throws StorageProviderException;

  /**
   * Returns the metadata properties for a remote storage object
   *
   * @param remoteStorageLocation location, i.e. bucket for S3
   * @param prefix the prefix/path of the object to retrieve
   * @return storage metadata object
   * @throws StorageProviderException cloud storage provider error
   */
  StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix)
      throws StorageProviderException;

  /**
   * Download a file from remote storage.
   *
   * @param connection connection object
   * @param command command to download file
   * @param localLocation local file path
   * @param destFileName destination file name
   * @param parallelism number of threads for parallel downloading
   * @param remoteStorageLocation remote storage location, i.e. bucket for S3
   * @param stageFilePath stage file path
   * @param stageRegion region name where the stage persists
   * @param presignedUrl presigned URL for download. Used by GCP.
   * @throws SnowflakeSQLException download failure
   */
  void download(
      SFSession connection,
      String command,
      String localLocation,
      String destFileName,
      int parallelism,
      String remoteStorageLocation,
      String stageFilePath,
      String stageRegion,
      String presignedUrl)
      throws SnowflakeSQLException;

  /**
   * Download a file from remote storage
   *
   * @param connection connection object
   * @param command command to download file
   * @param parallelism number of threads for parallel downloading
   * @param remoteStorageLocation remote storage location, i.e. bucket for s3
   * @param stageFilePath stage file path
   * @param stageRegion region name where the stage persists
   * @param presignedUrl presigned URL for download. Used by GCP.
   * @return input file stream
   * @throws SnowflakeSQLException when download failure
   */
  InputStream downloadToStream(
      SFSession connection,
      String command,
      int parallelism,
      String remoteStorageLocation,
      String stageFilePath,
      String stageRegion,
      String presignedUrl)
      throws SnowflakeSQLException;

  /**
   * Upload a file (-stream) to remote storage
   *
   * @param connection connection object
   * @param command upload command
   * @param parallelism number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl presigned URL for upload. Used by GCP.
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  void upload(
      SFSession connection,
      String command,
      int parallelism,
      boolean uploadFromStream,
      String remoteStorageLocation,
      File srcFile,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      StorageObjectMetadata meta,
      String stageRegion,
      String presignedUrl)
      throws SnowflakeSQLException;

  /**
   * Upload a file (-stream) to remote storage with Pre-signed URL without JDBC connection.
   *
   * <p>NOTE: This function is only supported when pre-signed URL is used.
   *
   * @param networkTimeoutInMilli Network timeout for the upload
   * @param ocspModeAndProxyKey OCSP mode and proxy settings for the upload.
   * @param parallelism number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl presigned URL for upload. Used by GCP.
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  default void uploadWithPresignedUrlWithoutConnection(
      int networkTimeoutInMilli,
      HttpClientSettingsKey ocspModeAndProxyKey,
      int parallelism,
      boolean uploadFromStream,
      String remoteStorageLocation,
      File srcFile,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutputStream,
      StorageObjectMetadata meta,
      String stageRegion,
      String presignedUrl)
      throws SnowflakeSQLException {
    if (!requirePresignedUrl()) {
      throw new SnowflakeSQLLoggedException(
          null,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          /*session = */ "uploadWithPresignedUrlWithoutConnection"
              + " only works for pre-signed URL.");
    }
  }

  /**
   * Handles exceptions thrown by the remote storage provider
   *
   * @param ex the exception to handle
   * @param retryCount current number of retries, incremented by the caller before each call
   * @param operation string that indicates the function/operation that was taking place, when the
   *     exception was raised, for example "upload"
   * @param connection the current SFSession object used by the client
   * @param command the command attempted at the time of the exception
   * @throws SnowflakeSQLException exceptions that were not handled, or retried past what the retry
   *     policy allows, are propagated
   */
  void handleStorageException(
      Exception ex, int retryCount, String operation, SFSession connection, String command)
      throws SnowflakeSQLException;

  /**
   * Returns the material descriptor key
   *
   * @return the material descriptor key
   */
  String getMatdescKey();

  /**
   * Adds encryption metadata to the StorageObjectMetadata object
   *
   * @param meta the storage metadata object to add the encyption info to
   * @param matDesc the material decriptor
   * @param ivData the initialization vector
   * @param encKeK the key encryption key
   * @param contentLength the length of the encrypted content
   */
  void addEncryptionMetadata(
      StorageObjectMetadata meta,
      MatDesc matDesc,
      byte[] ivData,
      byte[] encKeK,
      long contentLength);

  /**
   * Adds digest metadata to the StorageObjectMetadata object
   *
   * @param meta the storage metadata object to add the digest to
   * @param digest the digest metadata to add
   */
  void addDigestMetadata(StorageObjectMetadata meta, String digest);

  /**
   * Gets digest metadata to the StorageObjectMetadata object
   *
   * @param meta the metadata object to extract the digest metadata from
   * @return the digest metadata value
   */
  String getDigestMetadata(StorageObjectMetadata meta);

  /**
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   *
   * @param meta the storage metadata object to add the digest to
   * @param clientName streaming ingest client name
   * @param clientKey streaming ingest client key, provided by Snowflake
   */
  void addStreamingIngestMetadata(StorageObjectMetadata meta, String clientName, String clientKey);

  /** Gets streaming ingest client name to the StorageObjectMetadata object */
  String getStreamingIngestClientName(StorageObjectMetadata meta);

  /** Gets streaming ingest client key to the StorageObjectMetadata object */
  String getStreamingIngestClientKey(StorageObjectMetadata meta);
}
