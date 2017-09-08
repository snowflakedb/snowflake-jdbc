/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;
import net.snowflake.client.log.SFLogger;
import com.amazonaws.ClientConfiguration;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

import java.util.Map;

/**
 * Factory object for abstracting the creation of storage client objects:
 * SnowflakeStorageClient and StorageObjectMetadata
 *
 * @author lgiakoumakis
 */
public class StorageClientFactory
{

  private final static SFLogger logger =
          SFLoggerFactory.getLogger(SnowflakeS3Client.class);

  private static StorageClientFactory factory;

  private StorageClientFactory() {}

  /**
   * Creates or returns the single instance of the factory object
   * @return a factory object
   */
  public static StorageClientFactory getFactory()
  {
    if(factory == null) factory = new StorageClientFactory();

    return factory;
  }

  /**
   * Creates a storage client based on the value of stageLocationType
   *
   * @param stageLocationType The type of the stage, e.g. "S3"
   * @param stageCredentials Map of stage credential properties
   * @param parallel the degree of parallelism to be used by the client
   * @param encMat encryption material for the client
   * @param stageRegion the region where our stage is located
   * @return a SnowflakeStorageClient interface to the instance created
   * @throws SnowflakeSQLException if any error occurs
   */
  public SnowflakeStorageClient createClient(String stageLocationType,
                                             Map stageCredentials,
                                             int parallel,
                                             RemoteStoreFileEncryptionMaterial encMat,
                                             String stageRegion)
                                             throws SnowflakeSQLException
  {
    logger.debug("createClient client type={}", stageLocationType);

    if("S3".equalsIgnoreCase(stageLocationType))
    {
      return createS3Client(stageCredentials, parallel, encMat, stageRegion);
    }
    else
    {
      // We don't create a storage client for FS_LOCAL,
      // so we should only find ourselves here if an unsupported remote storage client type is specified
      throw new IllegalArgumentException("Unsupported storage client specified");
    }
  }

  /**
   * Creates a SnowflakeS3ClientObject which encapsulates
   * the Amazon S3 client
   *
   * @param stageCredentials Map of stage credential properties
   * @param parallel degree of parallelism
   * @param encMat encryption material for the client
   * @param stageRegion the region where the stage is located
   * @return the SnowflakeS3Client  instance created
   */
  private SnowflakeS3Client createS3Client(Map stageCredentials,
                                           int parallel,
                                           RemoteStoreFileEncryptionMaterial encMat,
                                           String stageRegion)
          throws SnowflakeSQLException
  {
    final int S3_TRANSFER_MAX_RETRIES = 3;

    logger.debug("createS3Client encryption={}", (encMat == null ? "no" : "yes"));

    SnowflakeS3Client s3Client;

    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setMaxConnections(parallel+1);
    clientConfig.setMaxErrorRetry(S3_TRANSFER_MAX_RETRIES);

    logger.debug("s3 client configuration: maxConnection={}, connectionTimeout={}, " +
                    "socketTimeout={}, maxErrorRetry={}",
            clientConfig.getMaxConnections(),
            clientConfig.getConnectionTimeout(),
            clientConfig.getSocketTimeout(),
            clientConfig.getMaxErrorRetry());

    try
    {
      s3Client = new SnowflakeS3Client(stageCredentials, clientConfig, encMat, stageRegion);
    }
    catch(Throwable ex)
    {
      logger.debug("Exception creating s3 client", ex);
      throw ex;
    }
    logger.debug("s3 client created");

    return s3Client;
  }

  /**
   * Creates a storage provider specific metadata object,
   * accessible via the platform independent interface
   *
   * @param stageLocationType determines the implementation to be created
   * @return the implementation of StorageObjectMetadata
   */
  public StorageObjectMetadata createStorageMetadataObj(String stageLocationType)
  {
    if("S3".equalsIgnoreCase(stageLocationType))
    {
      return new S3ObjectMetadata();
    }
    else
    {
      // An unsupported remote storage client type was specified
      // We don't create/implement a storage client for FS_LOCAL,
      // so we should never end up here while running on local file system
      throw new IllegalArgumentException("Unsupported stage type specified");
    }


  }
}
