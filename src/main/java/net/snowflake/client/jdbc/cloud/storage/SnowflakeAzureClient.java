package net.snowflake.client.jdbc.cloud.storage;

import static net.snowflake.client.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.client.core.HttpUtil.setProxyForAzure;
import static net.snowflake.client.core.HttpUtil.setSessionlessProxyForAzure;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAnonymous;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageExtendedErrorInformation;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SFPair;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;

/** Encapsulates the Azure Storage client and all Azure Storage operations and logic */
public class SnowflakeAzureClient implements SnowflakeStorageClient {

  private static final String localFileSep = systemGetProperty("file.separator");
  private static final String AZ_ENCRYPTIONDATAPROP = "encryptiondata";
  private static final String AZ_STREAMING_INGEST_CLIENT_NAME = "ingestclientname";
  private static final String AZ_STREAMING_INGEST_CLIENT_KEY = "ingestclientkey";

  private int encryptionKeySize = 0; // used for PUTs
  private StageInfo stageInfo;
  private RemoteStoreFileEncryptionMaterial encMat;
  private CloudBlobClient azStorageClient;
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeAzureClient.class);
  private OperationContext opContext = null;
  private SFBaseSession session;

  private SnowflakeAzureClient() {}
  ;

  /*
   * Factory method for a SnowflakeAzureClient object
   * @param stage   The stage information that the client will operate on
   * @param encMat  The encryption material
   *                required to decrypt/encrypt content in stage
   */
  public static SnowflakeAzureClient createSnowflakeAzureClient(
      StageInfo stage, RemoteStoreFileEncryptionMaterial encMat, SFBaseSession sfSession)
      throws SnowflakeSQLException {
    logger.debug(
        "Initializing Snowflake Azure client with encryption: {}",
        encMat != null ? "true" : "false");
    SnowflakeAzureClient azureClient = new SnowflakeAzureClient();
    azureClient.setupAzureClient(stage, encMat, sfSession);

    return azureClient;
  }

  /*
   * Initializes the Azure client
   * This method is used during the object construction, but also to
   * reset/recreate the encapsulated CloudBlobClient object with new
   * credentials (after SAS token expiration)
   * @param stage   The stage information that the client will operate on
   * @param encMat  The encryption material
   *                required to decrypt/encrypt content in stage
   * @throws IllegalArgumentException when invalid credentials are used
   */
  private void setupAzureClient(
      StageInfo stage, RemoteStoreFileEncryptionMaterial encMat, SFBaseSession sfSession)
      throws IllegalArgumentException, SnowflakeSQLException {
    // Save the client creation parameters so that we can reuse them,
    // to reset the Azure client.
    this.stageInfo = stage;
    this.encMat = encMat;
    this.session = sfSession;

    logger.debug("Setting up the Azure client ", false);

    try {
      URI storageEndpoint =
          buildAzureStorageEndpointURI(stage.getEndPoint(), stage.getStorageAccount());

      StorageCredentials azCreds;
      String sasToken = (String) stage.getCredentials().get("AZURE_SAS_TOKEN");
      if (sasToken != null) {
        // We are authenticated with a shared access token.
        azCreds = new StorageCredentialsSharedAccessSignature(sasToken);
      } else {
        // Use anonymous authentication.
        azCreds = StorageCredentialsAnonymous.ANONYMOUS;
      }

      if (stage.getIsClientSideEncrypted() && encMat != null) {
        byte[] decodedKey = Base64.getDecoder().decode(encMat.getQueryStageMasterKey());
        encryptionKeySize = decodedKey.length * 8;

        if (encryptionKeySize != 128 && encryptionKeySize != 192 && encryptionKeySize != 256) {
          throw new SnowflakeSQLLoggedException(
              QueryIdHelper.queryIdFromEncMatOr(encMat, null),
              session,
              ErrorCode.FILE_TRANSFER_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              "unsupported key size",
              encryptionKeySize);
        }
      }
      this.azStorageClient = new CloudBlobClient(storageEndpoint, azCreds);
      opContext = new OperationContext();
      if (session != null) {
        setProxyForAzure(session.getHttpClientKey(), opContext);
      } else {
        setSessionlessProxyForAzure(stage.getProxyProperties(), opContext);
      }
    } catch (URISyntaxException ex) {
      throw new IllegalArgumentException("invalid_azure_credentials");
    }
  }

  // Returns the Max number of retry attempts
  @Override
  public int getMaxRetries() {
    if (session != null
        && session
            .getConnectionPropertiesMap()
            .containsKey(SFSessionProperty.PUT_GET_MAX_RETRIES)) {
      return (int) session.getConnectionPropertiesMap().get(SFSessionProperty.PUT_GET_MAX_RETRIES);
    }
    return 25;
  }

  // Returns the max exponent for multiplying backoff with the power of 2, the value
  // of 4 will give us 16secs as the max number of time to sleep before retry
  @Override
  public int getRetryBackoffMaxExponent() {
    return 4;
  }

  // Returns the min number of milliseconds to sleep before retry
  @Override
  public int getRetryBackoffMin() {
    return 1000;
  }

  /**
   * @return Returns true if encryption is enabled
   */
  @Override
  public boolean isEncrypting() {
    return encryptionKeySize > 0 && this.stageInfo.getIsClientSideEncrypted();
  }

  /**
   * @return Returns the size of the encryption key
   */
  @Override
  public int getEncryptionKeySize() {
    return encryptionKeySize;
  }

  /**
   * Re-creates the encapsulated storage client with a fresh access token
   *
   * @param stageCredentials a Map (as returned by GS) which contains the new credential properties
   * @throws SnowflakeSQLException failure to renew the client
   */
  @Override
  public void renew(Map<?, ?> stageCredentials) throws SnowflakeSQLException {
    logger.debug("Renewing the Azure client");
    stageInfo.setCredentials(stageCredentials);
    setupAzureClient(stageInfo, encMat, session);
  }

  /** shuts down the client */
  @Override
  public void shutdown() {
    /* Not available */
  }

  /**
   * For a set of remote storage objects under a remote location and a given prefix/path returns
   * their properties wrapped in ObjectSummary objects
   *
   * @param remoteStorageLocation location, i.e. container for Azure
   * @param prefix the prefix/path to list under
   * @return a collection of storage summary objects
   * @throws StorageProviderException Azure storage exception
   */
  @Override
  public StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix)
      throws StorageProviderException {
    StorageObjectSummaryCollection storageObjectSummaries;

    try {
      CloudBlobContainer container = azStorageClient.getContainerReference(remoteStorageLocation);
      Iterable<ListBlobItem> listBlobItemIterable =
          container.listBlobs(
              prefix, // List the BLOBs under this prefix
              true, // List the BLOBs as a flat list, i.e. do not list directories
              EnumSet.noneOf(BlobListingDetails.class),
              (BlobRequestOptions) null,
              opContext);
      storageObjectSummaries = new StorageObjectSummaryCollection(listBlobItemIterable);
    } catch (URISyntaxException | StorageException ex) {
      logger.debug("Failed to list objects: {}", ex);
      throw new StorageProviderException(ex);
    }
    return storageObjectSummaries;
  }

  /**
   * Returns the metadata properties for a remote storage object
   *
   * @param remoteStorageLocation location, i.e. bucket for S3
   * @param prefix the prefix/path of the object to retrieve
   * @return storage metadata object
   * @throws StorageProviderException azure storage exception
   */
  @Override
  public StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix)
      throws StorageProviderException {
    CommonObjectMetadata azureObjectMetadata = null;
    try {
      // Get a reference to the BLOB, to retrieve its metadata
      CloudBlobContainer container = azStorageClient.getContainerReference(remoteStorageLocation);
      CloudBlob blob = container.getBlockBlobReference(prefix);
      blob.downloadAttributes(null, null, opContext);

      // Get the user-defined BLOB metadata
      Map<String, String> userDefinedMetadata =
          SnowflakeUtil.createCaseInsensitiveMap(blob.getMetadata());

      // Get the BLOB system properties we care about
      BlobProperties properties = blob.getProperties();
      long contentLength = properties.getLength();
      String contentEncoding = properties.getContentEncoding();

      // Construct an Azure metadata object
      azureObjectMetadata =
          new CommonObjectMetadata(contentLength, contentEncoding, userDefinedMetadata);
    } catch (StorageException ex) {
      logger.debug(
          "Failed to retrieve BLOB metadata: {} - {}",
          ex.getErrorCode(),
          FormatStorageExtendedErrorInformation(ex.getExtendedErrorInformation()));
      throw new StorageProviderException(ex);
    } catch (URISyntaxException ex) {
      logger.debug("Cannot retrieve BLOB properties, invalid URI: {}", ex);
      throw new StorageProviderException(ex);
    }

    return azureObjectMetadata;
  }

  /**
   * Download a file from remote storage.
   *
   * @param session session object
   * @param command command to download file
   * @param localLocation local file path
   * @param destFileName destination file name
   * @param parallelism number of threads for parallel downloading
   * @param remoteStorageLocation remote storage location, i.e. bucket for S3
   * @param stageFilePath stage file path
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Unused in Azure
   * @param queryId last query id
   * @throws SnowflakeSQLException download failure
   */
  @Override
  public void download(
      SFSession session,
      String command,
      String localLocation,
      String destFileName,
      int parallelism,
      String remoteStorageLocation,
      String stageFilePath,
      String stageRegion,
      String presignedUrl,
      String queryId)
      throws SnowflakeSQLException {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    String localFilePath = localLocation + localFileSep + destFileName;
    logger.debug(
        "Staring download of file from Azure stage path: {} to {}", stageFilePath, localFilePath);
    int retryCount = 0;
    do {
      try {
        File localFile = new File(localFilePath);
        CloudBlobContainer container = azStorageClient.getContainerReference(remoteStorageLocation);
        CloudBlob blob = container.getBlockBlobReference(stageFilePath);

        BlobRequestOptions transferOptions = new BlobRequestOptions();
        transferOptions.setConcurrentRequestCount(parallelism);

        blob.downloadToFile(localFilePath, null, transferOptions, opContext);
        SnowflakeUtil.assureOnlyUserAccessibleFilePermissions(
            localFile, session.isOwnerOnlyStageFilePermissionsEnabled());

        stopwatch.stop();
        long downloadMillis = stopwatch.elapsedMillis();

        // Pull object metadata from Azure
        blob.downloadAttributes(null, transferOptions, opContext);

        // Get the user-defined BLOB metadata
        Map<String, String> userDefinedMetadata =
            SnowflakeUtil.createCaseInsensitiveMap(blob.getMetadata());

        if (this.isEncrypting() && this.getEncryptionKeySize() <= 256) {
          if (!userDefinedMetadata.containsKey(AZ_ENCRYPTIONDATAPROP)) {
            throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Encryption data not found in the metadata of a file being downloaded");
          }
          AbstractMap.SimpleEntry<String, String> encryptionData =
              parseEncryptionData(userDefinedMetadata.get(AZ_ENCRYPTIONDATAPROP), queryId);

          String key = encryptionData.getKey();
          String iv = encryptionData.getValue();
          stopwatch.restart();
          if (key == null || iv == null) {
            throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "File metadata incomplete");
          }

          // Decrypt file
          try {
            EncryptionProvider.decrypt(localFile, key, iv, this.encMat);
            stopwatch.stop();
            long decryptMillis = stopwatch.elapsedMillis();
            logger.info(
                "Azure file {} downloaded to {}. It took {} ms (download: {} ms, decryption: {} ms) with {} retries",
                remoteStorageLocation,
                localFile.getAbsolutePath(),
                downloadMillis + decryptMillis,
                downloadMillis,
                decryptMillis,
                retryCount);
          } catch (Exception ex) {
            logger.error("Error decrypting file", ex);
            throw ex;
          }
        } else {
          logger.info(
              "Azure file {} downloaded to {}. It took {} ms with {} retries",
              remoteStorageLocation,
              localFile.getAbsolutePath(),
              downloadMillis,
              retryCount);
        }
        return;

      } catch (Exception ex) {
        logger.debug("Download unsuccessful {}", ex);
        handleAzureException(
            ex, ++retryCount, StorageHelper.DOWNLOAD, session, command, this, queryId);
      }
    } while (retryCount <= getMaxRetries());

    throw new SnowflakeSQLLoggedException(
        queryId,
        session,
        StorageHelper.getOperationException(StorageHelper.DOWNLOAD).getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "Unexpected: download unsuccessful without exception!");
  }

  /**
   * Download a file from remote storage
   *
   * @param session session object
   * @param command command to download file
   * @param parallelism number of threads for parallel downloading
   * @param remoteStorageLocation remote storage location, i.e. bucket for s3
   * @param stageFilePath stage file path
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Unused in Azure
   * @param queryId last query id
   * @return input file stream
   * @throws SnowflakeSQLException when download failure
   */
  @Override
  public InputStream downloadToStream(
      SFSession session,
      String command,
      int parallelism,
      String remoteStorageLocation,
      String stageFilePath,
      String stageRegion,
      String presignedUrl,
      String queryId)
      throws SnowflakeSQLException {
    logger.debug(
        "Staring download of file from Azure stage path: {} to input stream", stageFilePath);
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    int retryCount = 0;

    do {
      try {
        CloudBlobContainer container = azStorageClient.getContainerReference(remoteStorageLocation);

        CloudBlob blob = container.getBlockBlobReference(stageFilePath);

        InputStream stream = blob.openInputStream(null, null, opContext);
        stopwatch.stop();
        long downloadMillis = stopwatch.elapsedMillis();
        Map<String, String> userDefinedMetadata =
            SnowflakeUtil.createCaseInsensitiveMap(blob.getMetadata());

        if (this.isEncrypting() && this.getEncryptionKeySize() <= 256) {
          if (!userDefinedMetadata.containsKey(AZ_ENCRYPTIONDATAPROP)) {
            throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Encryption data not found in the metadata of a file being downloaded");
          }
          AbstractMap.SimpleEntry<String, String> encryptionData =
              parseEncryptionData(userDefinedMetadata.get(AZ_ENCRYPTIONDATAPROP), queryId);
          String key = encryptionData.getKey();
          String iv = encryptionData.getValue();
          stopwatch.restart();
          if (key == null || iv == null) {
            throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "File metadata incomplete");
          }

          try {
            InputStream is = EncryptionProvider.decryptStream(stream, key, iv, encMat);
            stopwatch.stop();
            long decryptMillis = stopwatch.elapsedMillis();
            logger.info(
                "Azure file {} downloaded to input stream. It took {} ms "
                    + "(download: {} ms, decryption: {} ms) with {} retries",
                stageFilePath,
                downloadMillis + decryptMillis,
                downloadMillis,
                decryptMillis,
                retryCount);
            return is;

          } catch (Exception ex) {
            logger.error("Error in decrypting file", ex);
            throw ex;
          }

        } else {
          logger.info(
              "Azure file {} downloaded to input stream. Download took {} ms with {} retries",
              stageFilePath,
              downloadMillis,
              retryCount);
          return stream;
        }

      } catch (Exception ex) {
        logger.debug("Downloading unsuccessful {}", ex);
        handleAzureException(
            ex, ++retryCount, StorageHelper.DOWNLOAD, session, command, this, queryId);
      }
    } while (retryCount < getMaxRetries());

    throw new SnowflakeSQLLoggedException(
        queryId,
        session,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "Unexpected: download unsuccessful without exception!");
  }

  /**
   * Upload a file/stream to remote storage
   *
   * @param session session object
   * @param command upload command
   * @param parallelism number of threads for parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation storage container name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on remote storage after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Unused in Azure
   * @param queryId last query id
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  @Override
  public void upload(
      SFSession session,
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
      String presignedUrl,
      String queryId)
      throws SnowflakeSQLException {
    logger.info(
        StorageHelper.getStartUploadLog(
            "Azure", uploadFromStream, inputStream, fileBackedOutputStream, srcFile, destFileName));
    final List<FileInputStream> toClose = new ArrayList<>();
    long originalContentLength = meta.getContentLength();

    SFPair<InputStream, Boolean> uploadStreamInfo =
        createUploadStream(
            srcFile,
            uploadFromStream,
            inputStream,
            meta,
            originalContentLength,
            fileBackedOutputStream,
            toClose,
            queryId);

    if (!(meta instanceof CommonObjectMetadata)) {
      throw new IllegalArgumentException("Unexpected metadata object type");
    }

    int retryCount = 0;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      try {
        InputStream fileInputStream = uploadStreamInfo.left;
        CloudBlobContainer container = azStorageClient.getContainerReference(remoteStorageLocation);
        CloudBlockBlob blob = container.getBlockBlobReference(destFileName);

        // Set the user-defined/Snowflake metadata and upload the BLOB
        blob.setMetadata(new HashMap<>(meta.getUserMetadata()));

        BlobRequestOptions transferOptions = new BlobRequestOptions();
        transferOptions.setConcurrentRequestCount(parallelism);

        blob.upload(
            fileInputStream, // input stream to upload from
            -1, // -1 indicates an unknown stream length
            null,
            transferOptions,
            opContext);
        stopwatch.stop();

        if (uploadFromStream) {
          logger.info(
              "Uploaded data from input stream to Azure location: {}. It took {} ms with {} retries",
              remoteStorageLocation,
              stopwatch.elapsedMillis(),
              retryCount);
        } else {
          logger.info(
              "Uploaded file {} to Azure location: {}. It took {} ms with {} retries",
              srcFile.getAbsolutePath(),
              remoteStorageLocation,
              stopwatch.elapsedMillis(),
              retryCount);
        }

        blob.uploadMetadata(null, transferOptions, opContext);

        // close any open streams in the "toClose" list and return
        for (FileInputStream is : toClose) {
          IOUtils.closeQuietly(is);
        }

        return;
      } catch (Exception ex) {
        handleAzureException(
            ex, ++retryCount, StorageHelper.UPLOAD, session, command, this, queryId);

        if (uploadFromStream && fileBackedOutputStream == null) {
          throw new SnowflakeSQLException(
              queryId,
              ex,
              SqlState.SYSTEM_ERROR,
              ErrorCode.UPLOAD_ERROR.getMessageCode(),
              "Encountered exception during upload: "
                  + ex.getMessage()
                  + "\nCannot retry upload from stream.");
        }
        uploadStreamInfo =
            createUploadStream(
                srcFile,
                uploadFromStream,
                inputStream,
                meta,
                originalContentLength,
                fileBackedOutputStream,
                toClose,
                queryId);
      }

    } while (retryCount <= getMaxRetries());

    for (FileInputStream is : toClose) {
      IOUtils.closeQuietly(is);
    }

    throw new SnowflakeSQLException(
        queryId,
        SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        "Unexpected: upload unsuccessful without exception!");
  }

  /**
   * Handles exceptions thrown by Azure Storage
   *
   * @param ex the exception to handle
   * @param retryCount current number of retries, incremented by the caller before each call
   * @param operation string that indicates the function/operation that was taking place, when the
   *     exception was raised, for example "upload"
   * @param session the current SFSession object used by the client
   * @param command the command attempted at the time of the exception
   * @param queryId last query id
   * @throws SnowflakeSQLException exceptions not handled
   */
  @Override
  public void handleStorageException(
      Exception ex,
      int retryCount,
      String operation,
      SFSession session,
      String command,
      String queryId)
      throws SnowflakeSQLException {
    handleAzureException(ex, retryCount, operation, session, command, this, queryId);
  }

  private SFPair<InputStream, Boolean> createUploadStream(
      File srcFile,
      boolean uploadFromStream,
      InputStream inputStream,
      StorageObjectMetadata meta,
      long originalContentLength,
      FileBackedOutputStream fileBackedOutputStream,
      List<FileInputStream> toClose,
      String queryId)
      throws SnowflakeSQLException {
    logger.debug(
        "createUploadStream({}, {}, {}, {}, {}, {})",
        this,
        srcFile,
        uploadFromStream,
        inputStream,
        fileBackedOutputStream,
        toClose);

    final InputStream stream;
    FileInputStream srcFileStream = null;
    try {
      if (isEncrypting() && getEncryptionKeySize() <= 256) {
        try {
          final InputStream uploadStream =
              uploadFromStream
                  ? (fileBackedOutputStream != null
                      ? fileBackedOutputStream.asByteSource().openStream()
                      : inputStream)
                  : (srcFileStream = new FileInputStream(srcFile));
          toClose.add(srcFileStream);

          // Encrypt
          stream =
              EncryptionProvider.encrypt(
                  meta, originalContentLength, uploadStream, this.encMat, this);
          uploadFromStream = true;
        } catch (Exception ex) {
          logger.error("Failed to encrypt input", ex);
          throw new SnowflakeSQLLoggedException(
              queryId,
              session,
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              ex,
              "Failed to encrypt input",
              ex.getMessage());
        }
      } else {
        if (uploadFromStream) {
          if (fileBackedOutputStream != null) {
            stream = fileBackedOutputStream.asByteSource().openStream();
          } else {
            stream = inputStream;
          }
        } else {
          srcFileStream = new FileInputStream(srcFile);
          toClose.add(srcFileStream);
          stream = srcFileStream;
        }
      }
    } catch (FileNotFoundException ex) {
      logger.error("Failed to open input file", ex);
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to open input file",
          ex.getMessage());
    } catch (IOException ex) {
      logger.error("Failed to open input stream", ex);
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to open input stream",
          ex.getMessage());
    }

    return SFPair.of(stream, uploadFromStream);
  }

  /**
   * Handles exceptions thrown by Azure Storage It will retry transient errors as defined by the
   * Azure Client retry policy It will re-create the client if the SAS token has expired, and re-try
   *
   * @param ex the exception to handle
   * @param retryCount current number of retries, incremented by the caller before each call
   * @param operation string that indicates the function/operation that was taking place, when the
   *     exception was raised, for example "upload"
   * @param session the current SFSession object used by the client
   * @param command the command attempted at the time of the exception
   * @param azClient the current Snowflake Azure client object
   * @throws SnowflakeSQLException exceptions not handled
   */
  private static void handleAzureException(
      Exception ex,
      int retryCount,
      String operation,
      SFSession session,
      String command,
      SnowflakeAzureClient azClient,
      String queryId)
      throws SnowflakeSQLException {

    // no need to retry if it is invalid key exception
    if (ex.getCause() instanceof InvalidKeyException) {
      // Most likely cause is that the unlimited strength policy files are not installed
      // Log the error and throw a message that explains the cause
      SnowflakeFileTransferAgent.throwJCEMissingError(operation, ex, queryId);
    }

    // If there is no space left in the download location, java.io.IOException is thrown.
    // Don't retry.
    if (SnowflakeUtil.getRootCause(ex) instanceof IOException) {
      SnowflakeFileTransferAgent.throwNoSpaceLeftError(session, operation, ex, queryId);
    }

    if (ex instanceof StorageException) {
      StorageException se = (StorageException) ex;

      if (((StorageException) ex).getHttpStatusCode() == 403) {
        // A 403 indicates that the SAS token has expired,
        // we need to refresh the Azure client with the new token
        if (session != null) {
          SnowflakeFileTransferAgent.renewExpiredToken(session, command, azClient);
        } else {
          // If session is null we cannot renew the token so throw the ExpiredToken exception
          throw new SnowflakeSQLException(
              queryId,
              se.getErrorCode(),
              CLOUD_STORAGE_CREDENTIALS_EXPIRED,
              "Azure credentials may have expired");
        }
      }
      // If we have exceeded the max number of retries, propagate the error
      // no need for back off and retry if the file does not exist
      if (retryCount > azClient.getMaxRetries()
          || ((StorageException) ex).getHttpStatusCode() == 404) {
        throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            SqlState.SYSTEM_ERROR,
            ErrorCode.AZURE_SERVICE_ERROR.getMessageCode(),
            se,
            operation,
            se.getErrorCode(),
            se.getHttpStatusCode(),
            se.getMessage(),
            FormatStorageExtendedErrorInformation(se.getExtendedErrorInformation()));
      } else {
        logger.debug(
            "Encountered exception ({}) during {}, retry count: {}",
            ex.getMessage(),
            operation,
            retryCount);
        logger.debug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis = azClient.getRetryBackoffMin();

        if (retryCount > 1) {
          backoffInMillis <<= (Math.min(retryCount - 1, azClient.getRetryBackoffMaxExponent()));
        }

        try {
          logger.debug("Sleep for {} milliseconds before retry", backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }

        if (se.getHttpStatusCode() == 403) {
          // A 403 indicates that the SAS token has expired,
          // we need to refresh the Azure client with the new token
          SnowflakeFileTransferAgent.renewExpiredToken(session, command, azClient);
        }
      }
    } else {
      if (ex instanceof InterruptedException
          || SnowflakeUtil.getRootCause(ex) instanceof SocketTimeoutException) {
        if (retryCount > azClient.getMaxRetries()) {
          throw new SnowflakeSQLLoggedException(
              queryId,
              session,
              SqlState.SYSTEM_ERROR,
              ErrorCode.FILE_TRANSFER_ERROR.getMessageCode(),
              ex,
              "Encountered exception during " + operation + ": " + ex.getMessage());
        } else {
          logger.debug(
              "Encountered exception ({}) during {}, retry count: {}",
              ex.getMessage(),
              operation,
              retryCount);
        }
      } else {
        throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            SqlState.SYSTEM_ERROR,
            ErrorCode.FILE_TRANSFER_ERROR.getMessageCode(),
            ex,
            "Encountered exception during " + operation + ": " + ex.getMessage());
      }
    }
  }

  /**
   * Format the StorageExtendedErrorInformation to a String.
   *
   * @param info the StorageExtendedErrorInformation object
   * @return
   */
  static String FormatStorageExtendedErrorInformation(StorageExtendedErrorInformation info) {
    if (info == null) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("StorageExceptionExtendedErrorInformation: {ErrorCode= ");
    sb.append(info.getErrorCode());
    sb.append(", ErrorMessage= ");
    sb.append(info.getErrorMessage());

    HashMap<String, String[]> details = info.getAdditionalDetails();
    if (details != null) {
      sb.append(", AdditionalDetails= { ");
      for (Map.Entry<String, String[]> detail : details.entrySet()) {
        sb.append(detail.getKey());
        sb.append("= ");

        for (String value : detail.getValue()) {
          sb.append(value);
        }
        sb.append(",");
      }
      sb.setCharAt(sb.length() - 1, '}'); // overwrite the last comma
    }
    sb.append("}");
    return sb.toString();
  }

  /*
   * Builds a URI to an Azure Storage account endpoint
   *
   *  @param storageEndPoint   the storage endpoint name
   *  @param storageAccount    the storage account name
   */
  private static URI buildAzureStorageEndpointURI(String storageEndPoint, String storageAccount)
      throws URISyntaxException {
    URI storageEndpoint =
        new URI("https", storageAccount + "." + storageEndPoint + "/", null, null);

    return storageEndpoint;
  }

  /*
   * buildEncryptionMetadataJSON
   * Takes the base64-encoded iv and key and creates the JSON block to be
   * used as the encryptiondata metadata field on the blob.
   */
  private String buildEncryptionMetadataJSON(String iv64, String key64) {
    return String.format(
        "{\"EncryptionMode\":\"FullBlob\",\"WrappedContentKey\""
            + ":{\"KeyId\":\"symmKey1\",\"EncryptedKey\":\"%s\""
            + ",\"Algorithm\":\"AES_CBC_256\"},\"EncryptionAgent\":"
            + "{\"Protocol\":\"1.0\",\"EncryptionAlgorithm\":"
            + "\"AES_CBC_256\"},\"ContentEncryptionIV\":\"%s\""
            + ",\"KeyWrappingMetadata\":{\"EncryptionLibrary\":"
            + "\"Java 5.3.0\"}}",
        key64, iv64);
  }

  /*
   * parseEncryptionData
   * Takes the json string in the encryptiondata metadata field of the encrypted
   * blob and parses out the key and iv. Returns the pair as key = key, iv = value.
   */
  private SimpleEntry<String, String> parseEncryptionData(String jsonEncryptionData, String queryId)
      throws SnowflakeSQLException {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    JsonFactory factory = mapper.getFactory();
    try {
      JsonParser parser = factory.createParser(jsonEncryptionData);
      JsonNode encryptionDataNode = mapper.readTree(parser);

      String iv = encryptionDataNode.get("ContentEncryptionIV").asText();
      String key = encryptionDataNode.get("WrappedContentKey").get("EncryptedKey").asText();

      return new SimpleEntry<String, String>(key, iv);
    } catch (Exception ex) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.SYSTEM_ERROR,
          ErrorCode.FILE_TRANSFER_ERROR.getMessageCode(),
          ex,
          "Error parsing encryption data as json" + ": " + ex.getMessage());
    }
  }

  /** Returns the material descriptor key */
  @Override
  public String getMatdescKey() {
    return "matdesc";
  }

  /** Adds encryption metadata to the StorageObjectMetadata object */
  @Override
  public void addEncryptionMetadata(
      StorageObjectMetadata meta,
      MatDesc matDesc,
      byte[] ivData,
      byte[] encryptedKey,
      long contentLength) {
    meta.addUserMetadata(getMatdescKey(), matDesc.toString());
    meta.addUserMetadata(
        AZ_ENCRYPTIONDATAPROP,
        buildEncryptionMetadataJSON(
            Base64.getEncoder().encodeToString(ivData),
            Base64.getEncoder().encodeToString(encryptedKey)));
    meta.setContentLength(contentLength);
  }

  /** Adds digest metadata to the StorageObjectMetadata object */
  @Override
  public void addDigestMetadata(StorageObjectMetadata meta, String digest) {
    if (!SnowflakeUtil.isBlank(digest)) {
      // Azure doesn't allow hyphens in the name of a metadata field.
      meta.addUserMetadata("sfcdigest", digest);
    }
  }

  /** Gets digest metadata to the StorageObjectMetadata object */
  @Override
  public String getDigestMetadata(StorageObjectMetadata meta) {
    return meta.getUserMetadata().get("sfcdigest");
  }

  /**
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   */
  @Override
  public void addStreamingIngestMetadata(
      StorageObjectMetadata meta, String clientName, String clientKey) {
    meta.addUserMetadata(AZ_STREAMING_INGEST_CLIENT_NAME, clientName);
    meta.addUserMetadata(AZ_STREAMING_INGEST_CLIENT_KEY, clientKey);
  }

  /** Gets streaming ingest client name to the StorageObjectMetadata object */
  @Override
  public String getStreamingIngestClientName(StorageObjectMetadata meta) {
    return meta.getUserMetadata().get(AZ_STREAMING_INGEST_CLIENT_NAME);
  }

  /** Gets streaming ingest client key to the StorageObjectMetadata object */
  @Override
  public String getStreamingIngestClientKey(StorageObjectMetadata meta) {
    return meta.getUserMetadata().get(AZ_STREAMING_INGEST_CLIENT_KEY);
  }
}
