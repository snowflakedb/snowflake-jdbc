package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.ClientConfiguration;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

/**
 * Factory object for abstracting the creation of storage client objects: SnowflakeStorageClient and
 * StorageObjectMetadata
 */
public class StorageClientFactory {

  private static final SFLogger logger = SFLoggerFactory.getLogger(StorageClientFactory.class);

  private static StorageClientFactory factory;

  private StorageClientFactory() {}

  /**
   * Creates or returns the single instance of the factory object
   *
   * @return the storage client instance
   */
  public static StorageClientFactory getFactory() {
    if (factory == null) {
      factory = new StorageClientFactory();
    }

    return factory;
  }

  /**
   * Creates a storage client based on the value of stageLocationType
   *
   * @param stage the stage properties
   * @param parallel the degree of parallelism to be used by the client
   * @param encMat encryption material for the client
   * @param session SFSession
   * @return a SnowflakeStorageClient interface to the instance created
   * @throws SnowflakeSQLException if any error occurs
   */
  public SnowflakeStorageClient createClient(
      StageInfo stage, int parallel, RemoteStoreFileEncryptionMaterial encMat, SFSession session)
      throws SnowflakeSQLException {
    logger.debug("Creating storage client. Client type: {}", stage.getStageType().name());

    switch (stage.getStageType()) {
      case S3:
        boolean useS3RegionalUrl =
            stage.getUseS3RegionalUrl()
                || stage.getUseRegionalUrl()
                || session != null && session.getUseRegionalS3EndpointsForPresignedURL();
        return createS3Client(
            stage.getCredentials(),
            parallel,
            encMat,
            stage.getProxyProperties(),
            stage.getRegion(),
            stage.getEndPoint(),
            stage.getIsClientSideEncrypted(),
            session,
            useS3RegionalUrl);

      case AZURE:
        return createAzureClient(stage, encMat, session);

      case GCS:
        return createGCSClient(stage, encMat, session);

      default:
        // We don't create a storage client for FS_LOCAL,
        // so we should only find ourselves here if an unsupported
        // remote storage client type is specified
        throw new IllegalArgumentException(
            "Unsupported storage client specified: " + stage.getStageType().name());
    }
  }

  /**
   * Creates a SnowflakeS3ClientObject which encapsulates the Amazon S3 client
   *
   * @param stageCredentials Map of stage credential properties
   * @param parallel degree of parallelism
   * @param encMat encryption material for the client
   * @param stageRegion the region where the stage is located
   * @param stageEndPoint the FIPS endpoint for the stage, if needed
   * @param isClientSideEncrypted whether client-side encryption should be used
   * @param session the active session
   * @param useS3RegionalUrl
   * @return the SnowflakeS3Client instance created
   * @throws SnowflakeSQLException failure to create the S3 client
   */
  private SnowflakeS3Client createS3Client(
      Map<?, ?> stageCredentials,
      int parallel,
      RemoteStoreFileEncryptionMaterial encMat,
      Properties proxyProperties,
      String stageRegion,
      String stageEndPoint,
      boolean isClientSideEncrypted,
      SFBaseSession session,
      boolean useS3RegionalUrl)
      throws SnowflakeSQLException {
    final int S3_TRANSFER_MAX_RETRIES = 3;

    logger.debug("Creating S3 client with encryption: {}", (encMat == null ? "no" : "yes"));

    SnowflakeS3Client s3Client;

    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setMaxConnections(parallel + 1);
    clientConfig.setMaxErrorRetry(S3_TRANSFER_MAX_RETRIES);
    clientConfig.setDisableSocketProxy(HttpUtil.isSocksProxyDisabled());

    // If proxy is set via connection properties or JVM settings these will be overridden later.
    // This is to prevent the aws client builder from reading proxy environment variables.
    clientConfig.setProxyHost("");
    clientConfig.setProxyPort(0);
    clientConfig.setProxyUsername("");
    clientConfig.setProxyPassword("");

    logger.debug(
        "S3 client configuration: maxConnection: {}, connectionTimeout: {}, "
            + "socketTimeout: {}, maxErrorRetry: {}",
        clientConfig.getMaxConnections(),
        clientConfig.getConnectionTimeout(),
        clientConfig.getSocketTimeout(),
        clientConfig.getMaxErrorRetry());

    try {
      s3Client =
          new SnowflakeS3Client(
              stageCredentials,
              clientConfig,
              encMat,
              proxyProperties,
              stageRegion,
              stageEndPoint,
              isClientSideEncrypted,
              session,
              useS3RegionalUrl);
    } catch (Exception ex) {
      logger.debug("Exception creating s3 client", ex);
      throw ex;
    }
    logger.debug("S3 Storage client created", false);

    return s3Client;
  }

  /**
   * Creates a storage provider specific metadata object, accessible via the platform independent
   * interface
   *
   * @param stageType determines the implementation to be created
   * @return the implementation of StorageObjectMetadata
   */
  public StorageObjectMetadata createStorageMetadataObj(StageInfo.StageType stageType) {
    switch (stageType) {
      case S3:
        return new S3ObjectMetadata();

      case AZURE:
      case GCS:
        // GCS's metadata object looks just like Azure's (Map<String, String>),
        // so for now we'll use the same class.
        return new CommonObjectMetadata();

      default:
        // An unsupported remote storage client type was specified
        // We don't create/implement a storage client for FS_LOCAL,
        // so we should never end up here while running on local file system
        throw new IllegalArgumentException("Unsupported stage type specified: " + stageType.name());
    }
  }

  /**
   * Creates a SnowflakeAzureClientObject which encapsulates the Azure Storage client
   *
   * @param stage Stage information
   * @param encMat encryption material for the client
   * @param session
   * @return the SnowflakeS3Client instance created
   */
  private SnowflakeAzureClient createAzureClient(
      StageInfo stage, RemoteStoreFileEncryptionMaterial encMat, SFBaseSession session)
      throws SnowflakeSQLException {
    logger.debug("Creating Azure client with encryption: {}", (encMat == null ? "no" : "yes"));

    SnowflakeAzureClient azureClient;

    try {
      azureClient = SnowflakeAzureClient.createSnowflakeAzureClient(stage, encMat, session);
    } catch (Exception ex) {
      logger.debug("Exception creating Azure Storage client", ex);
      throw ex;
    }
    logger.debug("Azure Storage client created", false);

    return azureClient;
  }

  /**
   * Creates a SnowflakeGCSClient object which encapsulates the GCS Storage client
   *
   * @param stage Stage information
   * @param encMat encryption material for the client
   * @return the SnowflakeGCSClient instance created
   */
  private SnowflakeGCSClient createGCSClient(
      StageInfo stage, RemoteStoreFileEncryptionMaterial encMat, SFSession session)
      throws SnowflakeSQLException {
    logger.debug("Creating GCS client with encryption: {}", (encMat == null ? "no" : "yes"));

    SnowflakeGCSClient gcsClient;

    try {
      gcsClient = SnowflakeGCSClient.createSnowflakeGCSClient(stage, encMat, session);
    } catch (Exception ex) {
      logger.debug("Exception creating GCS Storage client", ex);
      throw ex;
    }
    logger.debug("GCS Storage client created", false);

    return gcsClient;
  }
}
