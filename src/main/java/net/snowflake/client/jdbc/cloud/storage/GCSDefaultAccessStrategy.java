package net.snowflake.client.jdbc.cloud.storage;

import static net.snowflake.client.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;

import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Map;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SFPair;
import net.snowflake.common.core.SqlState;

class GCSDefaultAccessStrategy implements GCSAccessStrategy {
  private static final SFLogger logger = SFLoggerFactory.getLogger(GCSDefaultAccessStrategy.class);
  private Storage gcsClient = null;

  GCSDefaultAccessStrategy(StageInfo stage, SFSession session) {
    String accessToken = (String) stage.getCredentials().get("GCS_ACCESS_TOKEN");

    if (accessToken != null) {
      // We are authenticated with an oauth access token.
      StorageOptions.Builder builder = StorageOptions.newBuilder();
      overrideHost(stage, builder);

      if (SnowflakeGCSClient.areDisabledGcsDefaultCredentials(session)) {
        logger.debug(
            "Adding explicit credentials to avoid default credential lookup by the GCS client");
        builder.setCredentials(GoogleCredentials.create(new AccessToken(accessToken, null)));
      }

      // Using GoogleCredential with access token will cause IllegalStateException when the token
      // is expired and trying to refresh, which cause error cannot be caught. Instead, set a
      // header so we can caught the error code.
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
    if (ex instanceof StorageException) {
      // NOTE: this code path only handle Access token based operation,
      // presigned URL is not covered. Presigned Url do not raise
      // StorageException

      StorageException se = (StorageException) ex;
      // If we have exceeded the max number of retries, propagate the error
      if (retryCount > gcsClient.getMaxRetries()) {
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
            ex.getMessage(),
            operation,
            retryCount);
        logger.debug("Stack trace: ", ex);

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
      return false;
    }
  }

  @Override
  public void shutdown() {
    // nothing to do here
  }
}
