package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;

import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.Channels;
import java.util.Map;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.client.internal.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.client.internal.util.SFPair;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpStatus;

class GCSDefaultAccessStrategy implements GCSAccessStrategy {
  private static final SFLogger logger = SFLoggerFactory.getLogger(GCSDefaultAccessStrategy.class);
  private Storage gcsClient = null;

  GCSDefaultAccessStrategy(StageInfo stage, SFSession session) throws SnowflakeSQLException {
    HttpTransportFactory transportFactory;
    if (session != null) {
      transportFactory =
          CloudStorageProxyFactory.createHttpTransportForGCS(session.getHttpClientKey());
    } else {
      transportFactory =
          CloudStorageProxyFactory.createSessionlessHttpTransportForGCS(stage.getProxyProperties());
    }

    String accessToken = (String) stage.getCredentials().get("GCS_ACCESS_TOKEN");

    if (accessToken != null) {
      // We are authenticated with an oauth access token.
      StorageOptions.Builder builder = StorageOptions.newBuilder();
      overrideHost(stage, builder);

      // Always set explicit credentials to prevent Application Default Credentials (ADC) lookup.
      // Without this, StorageOptions.build().getService() probes metadata.google.internal which
      // is unreachable in environments like SPCS, causing an opaque "invalid_gcs_credentials"
      // error.
      // Actual API authentication is handled by the Authorization header below, not this credential
      // object — it exists solely to suppress the ADC probe.
      builder.setCredentials(GoogleCredentials.create(new AccessToken(accessToken, null)));

      if (transportFactory != null) {
        builder.setTransportOptions(
            HttpTransportOptions.newBuilder().setHttpTransportFactory(transportFactory).build());
      }
      this.gcsClient =
          builder
              .setHeaderProvider(
                  FixedHeaderProvider.create("Authorization", "Bearer " + accessToken))
              .build()
              .getService();
    } else {
      // Use anonymous authentication.
      HttpStorageOptions.Builder builder =
          HttpStorageOptions.newBuilder().setCredentials(NoCredentials.getInstance());
      overrideHost(stage, builder);
      if (transportFactory != null) {
        builder.setTransportOptions(
            HttpTransportOptions.newBuilder().setHttpTransportFactory(transportFactory).build());
      }
      this.gcsClient = builder.build().getService();
    }
  }

  private static void overrideHost(StageInfo stage, StorageOptions.Builder builder) {
    stage
        .gcsCustomEndpoint()
        .ifPresent(
            host -> {
              if (host.startsWith("https://")) {
                builder.setHost(host);
              } else {
                builder.setHost("https://" + host);
              }
            });
  }

  @Override
  public StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix) {
    try {
      logger.debug(
          "Listing objects in the bucket {} with prefix {}", remoteStorageLocation, prefix);
      Page<Blob> blobs =
          this.gcsClient.list(remoteStorageLocation, Storage.BlobListOption.prefix(prefix));
      return new StorageObjectSummaryCollection(blobs);
    } catch (Exception e) {
      logger.debug("Failed to list objects", false);
      throw new StorageProviderException(e);
    }
  }

  @Override
  public StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix) {
    try {
      BlobId blobId = BlobId.of(remoteStorageLocation, prefix);
      Blob blob = gcsClient.get(blobId);

      // GCS returns null if the blob was not found
      // By design, our storage platform expects to see a "blob not found" situation
      // as a RemoteStorageProviderException
      // Hence, we throw a RemoteStorageProviderException
      if (blob == null) {
        throw new StorageProviderException(
            new StorageException(
                404, // because blob not found
                "Blob" + blobId.getName() + " not found in bucket " + blobId.getBucket()));
      }

      return new CommonObjectMetadata(
          blob.getSize(), blob.getContentEncoding(), blob.getMetadata());
    } catch (StorageException ex) {
      throw new StorageProviderException(ex);
    }
  }

  @Override
  public Map<String, String> download(
      int parallelism, String remoteStorageLocation, String stageFilePath, File localFile) {
    BlobId blobId = BlobId.of(remoteStorageLocation, stageFilePath);
    Blob blob = gcsClient.get(blobId);
    if (blob == null) {
      throw new StorageProviderException(
          new StorageException(
              404, // because blob not found
              "Blob" + blobId.getName() + " not found in bucket " + blobId.getBucket()));
    }

    logger.debug("Starting download without presigned URL", false);
    blob.downloadTo(localFile.toPath(), Blob.BlobSourceOption.shouldReturnRawInputStream(true));

    // Get the user-defined BLOB metadata
    return SnowflakeUtil.createCaseInsensitiveMap(blob.getMetadata());
  }

  @Override
  public SFPair<InputStream, Map<String, String>> downloadToStream(
      String remoteStorageLocation, String stageFilePath, boolean isEncrypting) {
    BlobId blobId = BlobId.of(remoteStorageLocation, stageFilePath);
    Blob blob = gcsClient.get(blobId);
    if (blob == null) {
      throw new StorageProviderException(
          new StorageException(
              404, // because blob not found
              "Blob" + blobId.getName() + " not found in bucket " + blobId.getBucket()));
    }
    InputStream inputStream = Channels.newInputStream(blob.reader());
    Map<String, String> userDefinedMetadata = null;
    if (isEncrypting) {
      // Get the user-defined BLOB metadata
      userDefinedMetadata = SnowflakeUtil.createCaseInsensitiveMap(blob.getMetadata());
    }

    return SFPair.of(inputStream, userDefinedMetadata);
  }

  @Override
  public void uploadWithDownScopedToken(
      int parallelism,
      String remoteStorageLocation,
      String destFileName,
      String contentEncoding,
      Map<String, String> metadata,
      long contentLength,
      InputStream content,
      String queryId) {
    BlobId blobId = BlobId.of(remoteStorageLocation, destFileName);
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId)
            .setContentEncoding(contentEncoding)
            .setMetadata(metadata)
            .build();

    gcsClient.create(blobInfo, content);
  }

  @Override
  public boolean handleStorageException(
      Exception ex,
      int retryCount,
      String operation,
      SFSession session,
      String command,
      String queryId,
      SnowflakeGCSClient gcsClient)
      throws SnowflakeSQLException {
    // Find a StorageException either directly or anywhere in the cause chain.
    // The exception may arrive wrapped (e.g. in SnowflakeSQLException) when it originates from the
    // inner uploadWithDownScopedToken method, which catches the original StorageException and
    // re-wraps it before re-throwing. Walking the chain ensures that a transient 503 or similar
    // GCS error is always treated as retryable regardless of how many layers of wrapping were
    // added.
    StorageException se = SnowflakeUtil.findFirstCauseOfType(ex, StorageException.class);
    if (se != null && se != ex) {
      logger.debug(
          "GCSDefaultAccessStrategy: found StorageException (HTTP {}) wrapped inside {} during {};"
              + " treating as retryable",
          se.getCode(),
          ex.getClass().getSimpleName(),
          operation);
    }

    if (se != null) {
      // NOTE: this code path only handles Access token based operations.
      // Presigned URL operations do not raise StorageException.

      // Non-transient client errors must not be retried during upload/download, mirroring
      // S3ErrorHandler.isClientException400Or404 (400 and 404 only).
      // 401 is excluded here because it is handled below via token refresh.
      //
      // WHY THE SCOPE IS LIMITED TO UPLOAD AND DOWNLOAD:
      // When OVERWRITE=FALSE, SnowflakeFileTransferAgent.filterExistingFiles() calls
      // storageClient.listObjects() (returns an empty collection on miss — never a 404) and
      // then storageClient.getObjectMetadata() on each listed object to compare digests.
      // If the object disappears between the list and the metadata fetch (a race condition),
      // GCSDefaultAccessStrategy.getObjectMetadata() throws StorageProviderException wrapping a
      // synthetic StorageException(404). The SnowflakeFileTransferAgent.compareAndSkipRemoteFiles()
      // SNOW-14521 handler attempts to recognise this 404 via isServiceException404(), but that
      // method only checks SdkServiceException (AWS SDK); it does not recognise the GCS-native
      // StorageException, so the exception is re-thrown and eventually reaches
      // handleStorageException() with operation="compareRemoteFiles". Applying the
      // immediate-throw rule for 404 here would break that retry path — the correct behaviour is
      // to retry the list/compare cycle via the normal max-retries logic.
      // NOTE: for the upload path itself, OVERWRITE=FALSE produces only a LIST (GET ?prefix=...)
      // call and, if the file is found, skips the upload before getObjectMetadata() is ever
      // reached. No 404 is generated by the upload-path existence check.
      if ((StorageHelper.UPLOAD.equals(operation) || StorageHelper.DOWNLOAD.equals(operation))
          && (se.getCode() == HttpStatus.SC_BAD_REQUEST
              || se.getCode() == HttpStatus.SC_NOT_FOUND)) {
        throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            SqlState.SYSTEM_ERROR,
            StorageHelper.getOperationException(operation).getMessageCode(),
            se,
            operation,
            se.getCode(),
            se.getMessage(),
            se.getReason());
      }

      // If we have exceeded the max number of retries, propagate the error.
      if (retryCount > gcsClient.getMaxRetries()) {
        logger.error(
            "GCSDefaultAccessStrategy: max retries ({}) exceeded for StorageException"
                + " (HTTP {}, message: {}) during {}, giving up",
            gcsClient.getMaxRetries(),
            se.getCode(),
            se.getMessage(),
            operation,
            ex);
        throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            SqlState.SYSTEM_ERROR,
            StorageHelper.getOperationException(operation).getMessageCode(),
            se,
            operation,
            se.getCode(),
            se.getMessage(),
            se.getReason());
      } else {
        logger.debug(
            "Encountered exception ({}) during {}, retry count: {}",
            se.getMessage(),
            operation,
            retryCount,
            ex);

        // exponential backoff up to a limit
        int backoffInMillis = gcsClient.getRetryBackoffMin();

        if (retryCount > 1) {
          backoffInMillis <<= (Math.min(retryCount - 1, gcsClient.getRetryBackoffMaxExponent()));
        }

        try {
          logger.debug("Sleep for {} milliseconds before retry", backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }

        if (se.getCode() == 401 && command != null) {
          if (session != null) {
            // A 401 indicates that the access token has expired,
            // we need to refresh the GCS client with the new token
            SnowflakeFileTransferAgent.renewExpiredToken(session, command, gcsClient);
          } else {
            throw new SnowflakeSQLException(
                queryId,
                se.getMessage(),
                CLOUD_STORAGE_CREDENTIALS_EXPIRED,
                "GCS credentials have expired");
          }
        }
      }
      return true;
    } else {
      // InterruptedException and SocketTimeoutException are handled by the outer retry loop in
      // SnowflakeGCSClient.handleStorageException (retry within max retries, throw when exhausted).
      // Return false so that logic is preserved rather than bypassed.
      if (ex instanceof InterruptedException
          || SnowflakeUtil.getRootCause(ex) instanceof SocketTimeoutException) {
        return false;
      }
      logger.error(
          "GCSDefaultAccessStrategy: unhandled exception type {} during {}, not retrying",
          ex.getClass().getName(),
          operation,
          ex);
      // Re-throw as-is to avoid double-wrapping an already well-formed SQL exception.
      if (ex instanceof SnowflakeSQLException) {
        throw (SnowflakeSQLException) ex;
      }
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.SYSTEM_ERROR,
          StorageHelper.getOperationException(operation).getMessageCode(),
          ex,
          "Encountered exception during " + operation + ": " + ex.getMessage());
    }
  }

  @Override
  public void shutdown() {
    // nothing to do here
  }
}
