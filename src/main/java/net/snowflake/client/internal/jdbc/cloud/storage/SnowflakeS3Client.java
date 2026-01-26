package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.createDefaultExecutorService;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.getRootCause;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;
import static net.snowflake.client.internal.jdbc.cloud.storage.S3ErrorHandler.retryRequestWithExponentialBackoff;
import static net.snowflake.client.internal.jdbc.cloud.storage.S3ErrorHandler.throwIfClientExceptionOrMaxRetryReached;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.api.http.HttpHeadersCustomizer;
import net.snowflake.client.internal.core.HeaderCustomizerHttpRequestInterceptor;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFSessionProperty;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.client.internal.jdbc.FileBackedOutputStream;
import net.snowflake.client.internal.jdbc.MatDesc;
import net.snowflake.client.internal.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.client.internal.util.SFPair;
import net.snowflake.client.internal.util.Stopwatch;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.encryption.s3.CommitmentPolicy;
import software.amazon.encryption.s3.S3AsyncEncryptionClient;
import software.amazon.encryption.s3.algorithms.AlgorithmSuite;

/** Wrapper around AmazonS3Client. */
public class SnowflakeS3Client implements SnowflakeStorageClient {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeS3Client.class);
  private static final String localFileSep = systemGetProperty("file.separator");
  private static final String AES = "AES";
  private static final String AMZ_KEY = "x-amz-key";
  private static final String AMZ_IV = "x-amz-iv";
  private static final String S3_STREAMING_INGEST_CLIENT_NAME = "ingestclientname";
  private static final String S3_STREAMING_INGEST_CLIENT_KEY = "ingestclientkey";

  // expired AWS token error code
  protected static final String EXPIRED_AWS_TOKEN_ERROR_CODE = "ExpiredToken";

  private int encryptionKeySize = 0; // used for PUTs
  private S3AsyncClient amazonClient = null;
  private RemoteStoreFileEncryptionMaterial encMat = null;
  private ClientConfiguration clientConfig = null;
  private Properties proxyProperties = null;
  private String stageRegion = null;
  private String stageEndPoint = null; // FIPS endpoint, if needed
  private SFBaseSession session = null;
  private boolean isClientSideEncrypted = true;
  private boolean isUseS3RegionalUrl = false;

  public SnowflakeS3Client(
          Map<?, ?> stageCredentials,
          ClientConfiguration clientConfig,
          RemoteStoreFileEncryptionMaterial encMat,
          Properties proxyProperties,
          String stageRegion,
          String stageEndPoint,
          boolean isClientSideEncrypted,
          SFBaseSession session,
          boolean useS3RegionalUrl)
          throws SnowflakeSQLException {
    logger.debug(
            "Initializing Snowflake S3 client with encryption: {}, client side encrypted: {}",
            encMat != null,
            isClientSideEncrypted);
    this.session = session;
    this.isUseS3RegionalUrl = useS3RegionalUrl;
    setupSnowflakeS3Client(
            stageCredentials,
            clientConfig,
            encMat,
            proxyProperties,
            stageRegion,
            stageEndPoint,
            isClientSideEncrypted,
            session);
  }

  private void setupSnowflakeS3Client(
          Map<?, ?> stageCredentials,
          ClientConfiguration clientConfig,
          RemoteStoreFileEncryptionMaterial encMat,
          Properties proxyProperties,
          String stageRegion,
          String stageEndPoint,
          boolean isClientSideEncrypted,
          SFBaseSession session)
          throws SnowflakeSQLException {
    // Save the client creation parameters so that we can reuse them,
    // to reset the AWS client. We won't save the awsCredentials since
    // we will be refreshing that, every time we reset the AWS client
    this.clientConfig = clientConfig;
    this.stageRegion = stageRegion;
    this.encMat = encMat;
    this.proxyProperties = proxyProperties;
    this.stageEndPoint = stageEndPoint; // FIPS endpoint, if needed
    this.session = session;
    this.isClientSideEncrypted = isClientSideEncrypted;

    logger.debug("Setting up AWS client ", false);

    // Retrieve S3 stage credentials
    String awsID = (String) stageCredentials.get("AWS_KEY_ID");
    String awsKey = (String) stageCredentials.get("AWS_SECRET_KEY");
    String awsToken = (String) stageCredentials.get("AWS_TOKEN");

    // initialize aws credentials
    AwsCredentials awsCredentials =
            (awsToken != null)
                    ? AwsSessionCredentials.create(awsID, awsKey, awsToken)
                    : AwsBasicCredentials.create(awsID, awsKey);

    ProxyConfiguration proxyConfiguration;
    if (session != null) {
      proxyConfiguration = S3HttpUtil.createProxyConfigurationForS3(session.getHttpClientKey());
    } else {
      proxyConfiguration = S3HttpUtil.createSessionlessProxyConfigurationForS3(proxyProperties);
    }

    S3AsyncClientBuilder clientBuilder =
            S3AsyncClient.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials));

    Region region = Region.of(stageRegion);
    if (this.stageEndPoint != null
            && !this.stageEndPoint.isEmpty()
            && !"null".equals(this.stageEndPoint)) {
      clientBuilder.endpointOverride(URI.create(this.stageEndPoint));
      clientBuilder.region(region);
    } else {
      if (this.isUseS3RegionalUrl) {
        String domainSuffixForRegionalUrl = getDomainSuffixForRegionalUrl(region.id());
        String regionalEndpoint = "https://s3." + region.id() + "." + domainSuffixForRegionalUrl;
        clientBuilder.endpointOverride(URI.create(regionalEndpoint));
        clientBuilder.region(region);
      } else {
        clientBuilder.region(region);
      }
    }
    // Explicitly force to use virtual address style
    clientBuilder.forcePathStyle(false);

    clientBuilder.httpClientBuilder(
            NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(clientConfig.getMaxConnections())
                    .connectionAcquisitionTimeout(Duration.ofSeconds(60))
                    .proxyConfiguration(proxyConfiguration)
                    .connectionTimeout(Duration.ofMillis(clientConfig.connectionTimeout))
                    .readTimeout(Duration.ofMillis(clientConfig.socketTimeout))
                    .writeTimeout(Duration.ofMillis(clientConfig.socketTimeout)));
    clientBuilder.multipartEnabled(true);

    ClientOverrideConfiguration.Builder configurationBuilder =
            ClientOverrideConfiguration.builder();

    if (session instanceof SFSession) {
      List<HttpHeadersCustomizer> headersCustomizers =
              ((SFSession) session).getHttpHeadersCustomizers();
      if (headersCustomizers != null && !headersCustomizers.isEmpty()) {
        configurationBuilder.addExecutionInterceptor(
                new HeaderCustomizerHttpRequestInterceptor(headersCustomizers));
      }
    }

    clientBuilder.overrideConfiguration(configurationBuilder.build());

    if (encMat != null) {
      byte[] decodedKey = Base64.getDecoder().decode(encMat.getQueryStageMasterKey());
      encryptionKeySize = decodedKey.length * 8;

      if (encryptionKeySize == 256) {
        SecretKey queryStageMasterKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, AES);

        amazonClient =
                S3AsyncEncryptionClient.builderV4()
                        .wrappedClient(clientBuilder.build())
                        .aesKey(queryStageMasterKey)
                        /*
                         * migration from v3
                         * https://docs.aws.amazon.com/amazon-s3-encryption-client/latest/developerguide/java-v4-migration.html#java-v4-migration-migrate
                         * https://docs.aws.amazon.com/amazon-s3-encryption-client/latest/developerguide/java-v3-migration.html#v3-transitional-api-changes
                         */
                        // v3 default
                        .encryptionAlgorithm(AlgorithmSuite.ALG_AES_256_GCM_IV12_TAG16_NO_KDF)
                        // write: v3 compatible format, no key commitment
                        // read: both v3 and v4 format
                        .commitmentPolicy(CommitmentPolicy.FORBID_ENCRYPT_ALLOW_DECRYPT)
                        .build();
      } else if (encryptionKeySize == 128) {
        amazonClient = clientBuilder.build();
      } else {
        throw new SnowflakeSQLLoggedException(
                QueryIdHelper.queryIdFromEncMatOr(encMat, null),
                session,
                ErrorCode.FILE_TRANSFER_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "unsupported key size",
                encryptionKeySize);
      }
    } else {
      amazonClient = clientBuilder.build();
    }
  }

  static String getDomainSuffixForRegionalUrl(String regionName) {
    return regionName.toLowerCase().startsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
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

  @Override
  public boolean isEncrypting() {
    return encryptionKeySize > 0 && isClientSideEncrypted;
  }

  @Override
  public int getEncryptionKeySize() {
    return encryptionKeySize;
  }

  /**
   * Renew the S3 client with fresh AWS credentials/access token
   *
   * @param stageCredentials a Map of new AWS credential properties, to refresh the client with (as
   *     returned by GS)
   * @throws SnowflakeSQLException if any error occurs
   */
  @Override
  public void renew(Map<?, ?> stageCredentials) throws SnowflakeSQLException {
    logger.debug("Renewing the Snowflake S3 client");
    // We renew the client with fresh credentials and with its original parameters
    setupSnowflakeS3Client(
            stageCredentials,
            this.clientConfig,
            this.encMat,
            this.proxyProperties,
            this.stageRegion,
            this.stageEndPoint,
            this.isClientSideEncrypted,
            this.session);
  }

  @Override
  public void shutdown() {
    logger.debug("Shutting down the Snowflake S3 client");
    amazonClient.close();
  }

  @Override
  public StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix)
          throws StorageProviderException {
    ListObjectsResponse objListing =
            amazonClient
                    .listObjects(
                            ListObjectsRequest.builder().bucket(remoteStorageLocation).prefix(prefix).build())
                    .join();

    return new StorageObjectSummaryCollection(objListing.contents(), remoteStorageLocation);
  }

  @Override
  public StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix)
          throws StorageProviderException {
    return new S3ObjectMetadata(
            amazonClient
                    .headObject(
                            HeadObjectRequest.builder().bucket(remoteStorageLocation).key(prefix).build())
                    .join());
  }

  /**
   * Download a file from S3.
   *
   * @param session session object
   * @param command command to download file
   * @param localLocation local file path
   * @param destFileName destination file name
   * @param parallelism number of threads for parallel downloading
   * @param remoteStorageLocation s3 bucket name
   * @param stageFilePath stage file path
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Not used in S3
   * @param queryId last query id
   * @throws SnowflakeSQLException if download failed without an exception
   * @throws SnowflakeSQLException if failed to decrypt downloaded file
   * @throws SnowflakeSQLException if file metadata is incomplete
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
            "Staring download of file from S3 stage path: {} to {}", stageFilePath, localFilePath);
    S3TransferManager tx = null;
    int retryCount = 0;
    do {
      try {
        File localFile = new File(localFilePath);

        logger.debug("Creating executor service for transfer manager with {} threads", parallelism);

        // download files from s3
        tx =
                S3TransferManager.builder()
                        .s3Client(amazonClient)
                        .executor(
                                createDefaultExecutorService("s3-transfer-manager-downloader-", parallelism))
                        .build();

        FileDownload fileDownload =
                tx.downloadFile(
                        DownloadFileRequest.builder()
                                .getObjectRequest(
                                        GetObjectRequest.builder()
                                                .bucket(remoteStorageLocation)
                                                .key(stageFilePath)
                                                .build())
                                .destination(localFile.toPath())
                                .build());

        // Pull object metadata from S3
        CompletableFuture<HeadObjectResponse> metaFuture =
                amazonClient.headObject(
                        HeadObjectRequest.builder()
                                .bucket(remoteStorageLocation)
                                .key(stageFilePath)
                                .build());

        fileDownload.completionFuture().join();
        HeadObjectResponse meta = metaFuture.join();

        Map<String, String> metaMap = SnowflakeUtil.createCaseInsensitiveMap(meta.metadata());
        String key = metaMap.get(AMZ_KEY);
        String iv = metaMap.get(AMZ_IV);

        SnowflakeUtil.assureOnlyUserAccessibleFilePermissions(
                localFile, session.isOwnerOnlyStageFilePermissionsEnabled());
        stopwatch.stop();
        long downloadMillis = stopwatch.elapsedMillis();

        if (this.isEncrypting() && this.getEncryptionKeySize() < 256) {
          stopwatch.restart();
          if (key == null || iv == null) {
            throw new SnowflakeSQLLoggedException(
                    queryId,
                    session,
                    StorageHelper.getOperationException(StorageHelper.DOWNLOAD).getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    "File metadata incomplete");
          }

          // Decrypt file
          try {
            EncryptionProvider.decrypt(localFile, key, iv, this.encMat);
            stopwatch.stop();
            long decryptMillis = stopwatch.elapsedMillis();
            logger.info(
                    "S3 file {} downloaded to {}. It took {} ms (download: {} ms, decryption: {} ms) with {} retries",
                    stageFilePath,
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
                  "S3 file {} downloaded to {}. It took {} ms with {} retries",
                  stageFilePath,
                  localFile.getAbsolutePath(),
                  downloadMillis,
                  retryCount);
        }

        return;
      } catch (Exception ex) {
        handleS3Exception(
                ex, ++retryCount, StorageHelper.DOWNLOAD, session, command, this, queryId);

      } finally {
        if (tx != null) {
          tx.close();
        }
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
   * @param presignedUrl Not used in S3
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
    logger.debug("Starting download of file from S3 stage path: {} to input stream", stageFilePath);
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    int retryCount = 0;
    do {
      try {
        CompletableFuture<ResponseInputStream<GetObjectResponse>> streamFuture =
                amazonClient.getObject(
                        GetObjectRequest.builder().bucket(remoteStorageLocation).key(stageFilePath).build(),
                        AsyncResponseTransformer.toBlockingInputStream());
        CompletableFuture<HeadObjectResponse> metaFuture =
                amazonClient.headObject(
                        HeadObjectRequest.builder()
                                .bucket(remoteStorageLocation)
                                .key(stageFilePath)
                                .build());

        HeadObjectResponse meta = metaFuture.join();
        InputStream stream = streamFuture.join();
        stopwatch.stop();
        long downloadMillis = stopwatch.elapsedMillis();
        Map<String, String> metaMap = SnowflakeUtil.createCaseInsensitiveMap(meta.metadata());

        String key = metaMap.get(AMZ_KEY);
        String iv = metaMap.get(AMZ_IV);

        if (this.isEncrypting() && this.getEncryptionKeySize() < 256) {
          stopwatch.restart();
          if (key == null || iv == null) {
            throw new SnowflakeSQLLoggedException(
                    queryId,
                    session,
                    StorageHelper.getOperationException(StorageHelper.DOWNLOAD).getMessageCode(),
                    SqlState.INTERNAL_ERROR,
                    "File metadata incomplete");
          }

          try {
            InputStream is = EncryptionProvider.decryptStream(stream, key, iv, encMat);
            stopwatch.stop();
            long decryptMillis = stopwatch.elapsedMillis();
            logger.info(
                    "S3 file {} downloaded to input stream. It took {} ms "
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
                  "S3 file {} downloaded to input stream. Download took {} ms with {} retries",
                  stageFilePath,
                  downloadMillis,
                  retryCount);
        }
        return stream;
      } catch (Exception ex) {
        handleS3Exception(
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
   * Upload a file (-stream) to S3.
   *
   * @param session session object
   * @param command upload command
   * @param parallelism number of threads do parallel uploading
   * @param uploadFromStream true if upload source is stream
   * @param remoteStorageLocation s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on s3 after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @param stageRegion region name where the stage persists
   * @param presignedUrl Not used in S3
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
                    "S3", uploadFromStream, inputStream, fileBackedOutputStream, srcFile, destFileName));

    final long originalContentLength = meta.getContentLength();
    final List<FileInputStream> toClose = new ArrayList<>();
    SFPair<InputStream, Boolean> uploadStreamInfo =
            createUploadStream(
                    srcFile,
                    uploadFromStream,
                    inputStream,
                    fileBackedOutputStream,
                    meta,
                    originalContentLength,
                    toClose,
                    queryId);

    S3TransferManager tx = null;
    int retryCount = 0;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      try {
        PutObjectRequest.Builder putRequestBuilder =
                ((S3ObjectMetadata) meta)
                        .getS3PutObjectRequest().toBuilder()
                        .bucket(remoteStorageLocation)
                        .key(destFileName);

        logger.debug(
                "Creating executor service for transfer" + "manager with {} threads", parallelism);

        ThreadPoolExecutor executorService =
                createDefaultExecutorService("s3-transfer-manager-uploader-", parallelism);
        // upload files to s3
        tx = S3TransferManager.builder().s3Client(amazonClient).executor(executorService).build();

        final Upload myUpload;

        if (!this.isClientSideEncrypted) {
          // since we're not client-side encrypting, make sure we're server-side encrypting with
          // SSE-S3
          putRequestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        }

        PutObjectRequest request = putRequestBuilder.build();

        if (uploadStreamInfo.right) {
          myUpload =
                  tx.upload(
                          UploadRequest.builder()
                                  .putObjectRequest(request)
                                  .requestBody(
                                          AsyncRequestBody.fromInputStream(
                                                  uploadStreamInfo.left, request.contentLength(), executorService))
                                  .build());
        } else {
          myUpload =
                  tx.upload(
                          UploadRequest.builder()
                                  .putObjectRequest(request)
                                  .requestBody(AsyncRequestBody.fromFile(srcFile))
                                  .build());
        }

        myUpload.completionFuture().join();
        stopwatch.stop();
        long uploadMillis = stopwatch.elapsedMillis();

        // get out
        for (FileInputStream is : toClose) {
          IOUtils.closeQuietly(is);
        }

        if (uploadFromStream) {
          logger.info(
                  "Uploaded data from input stream to S3 location: {}. It took {} ms with {} retries",
                  destFileName,
                  uploadMillis,
                  retryCount);
        } else {
          logger.info(
                  "Uploaded file {} to S3 location: {}. It took {} ms with {} retries",
                  srcFile.getAbsolutePath(),
                  destFileName,
                  uploadMillis,
                  retryCount);
        }
        return;
      } catch (Exception ex) {
        handleS3Exception(ex, ++retryCount, StorageHelper.UPLOAD, session, command, this, queryId);
        if (uploadFromStream && fileBackedOutputStream == null) {
          throw new SnowflakeSQLException(
                  queryId,
                  ex,
                  SqlState.SYSTEM_ERROR,
                  ErrorCode.IO_ERROR.getMessageCode(),
                  "Encountered exception during upload: "
                          + ex.getMessage()
                          + "\nCannot retry upload from stream.");
        }
        uploadStreamInfo =
                createUploadStream(
                        srcFile,
                        uploadFromStream,
                        inputStream,
                        fileBackedOutputStream,
                        meta,
                        originalContentLength,
                        toClose,
                        queryId);
      } finally {
        if (tx != null) {
          tx.close();
        }
      }
    } while (retryCount <= getMaxRetries());

    for (FileInputStream is : toClose) {
      IOUtils.closeQuietly(is);
    }

    throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            StorageHelper.getOperationException(StorageHelper.UPLOAD).getMessageCode(),
            SqlState.INTERNAL_ERROR,
            "Unexpected: upload unsuccessful without exception!");
  }

  private SFPair<InputStream, Boolean> createUploadStream(
          File srcFile,
          boolean uploadFromStream,
          InputStream inputStream,
          FileBackedOutputStream fileBackedOutputStream,
          StorageObjectMetadata meta,
          long originalContentLength,
          List<FileInputStream> toClose,
          String queryId)
          throws SnowflakeSQLException {
    logger.debug(
            "createUploadStream({}, {}, {}, {}, {}, {}, {}) " + "keySize: {}",
            this,
            srcFile,
            uploadFromStream,
            inputStream,
            fileBackedOutputStream,
            meta,
            toClose,
            this.getEncryptionKeySize());
    final InputStream result;
    FileInputStream srcFileStream = null;
    if (isEncrypting() && getEncryptionKeySize() < 256) {
      try {
        final InputStream uploadStream =
                uploadFromStream
                        ? (fileBackedOutputStream != null
                        ? fileBackedOutputStream.asByteSource().openStream()
                        : inputStream)
                        : (srcFileStream = new FileInputStream(srcFile));
        toClose.add(srcFileStream);

        // Encrypt
        result =
                EncryptionProvider.encrypt(
                        meta, originalContentLength, uploadStream, this.encMat, this);
        uploadFromStream = true;
      } catch (Exception ex) {
        logger.error("Failed to encrypt input", ex);
        throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                SqlState.INTERNAL_ERROR,
                StorageHelper.getOperationException(StorageHelper.UPLOAD).getMessageCode(),
                ex,
                "Failed to encrypt input",
                ex.getMessage());
      }
    } else {
      try {
        result =
                uploadFromStream
                        ? (fileBackedOutputStream != null
                        ? fileBackedOutputStream.asByteSource().openStream()
                        : inputStream)
                        : (srcFileStream = new FileInputStream(srcFile));
        toClose.add(srcFileStream);

      } catch (FileNotFoundException ex) {
        logger.error("Failed to open input file", ex);
        throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                SqlState.INTERNAL_ERROR,
                StorageHelper.getOperationException(StorageHelper.UPLOAD).getMessageCode(),
                ex,
                "Failed to open input file",
                ex.getMessage());
      } catch (IOException ex) {
        logger.error("Failed to open input stream", ex);
        throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                SqlState.INTERNAL_ERROR,
                StorageHelper.getOperationException(StorageHelper.UPLOAD).getMessageCode(),
                ex,
                "Failed to open input stream",
                ex.getMessage());
      }
    }
    return SFPair.of(result, uploadFromStream);
  }

  @Override
  public void handleStorageException(
          Exception ex,
          int retryCount,
          String operation,
          SFSession session,
          String command,
          String queryId)
          throws SnowflakeSQLException {
    handleS3Exception(ex, retryCount, operation, session, command, this, queryId);
  }

  private static void handleS3Exception(
          Exception ex,
          int retryCount,
          String operation,
          SFSession session,
          String command,
          SnowflakeS3Client s3Client,
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
    if (getRootCause(ex) instanceof IOException) {
      SnowflakeFileTransferAgent.throwNoSpaceLeftError(session, operation, ex, queryId);
    }

    // Don't retry if max retries has been reached or the error code is 404/400
    Throwable cause = ex.getCause();
    if (cause instanceof SdkException) {
      logger.debug("SdkException: " + ex.getMessage());
      if (retryCount > s3Client.getMaxRetries() || s3Client.isClientException400Or404(cause)) {
        throwIfClientExceptionOrMaxRetryReached(
                operation, session, command, queryId, s3Client, cause);
      } else {
        retryRequestWithExponentialBackoff(
                ex, retryCount, operation, session, command, s3Client, queryId, cause);
      }
    } else {
      if (ex instanceof InterruptedException
              || getRootCause(ex) instanceof SocketTimeoutException
              || ex instanceof CompletionException) {
        if (retryCount > s3Client.getMaxRetries()) {
          throw new SnowflakeSQLLoggedException(
                  queryId,
                  session,
                  SqlState.SYSTEM_ERROR,
                  StorageHelper.getOperationException(operation).getMessageCode(),
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
                StorageHelper.getOperationException(operation).getMessageCode(),
                ex,
                "Encountered exception during " + operation + ": " + ex.getMessage());
      }
    }
  }

  /**
   * Checks the status code of the exception to see if it's a 400 or 404
   *
   * @param ex exception
   * @return true if it's a 400 or 404 status code
   */
  public boolean isClientException400Or404(Throwable ex) {
    if (ex instanceof SdkServiceException) {
      SdkServiceException asEx = (SdkServiceException) (ex);
      return asEx.statusCode() == HttpStatus.SC_NOT_FOUND
              || asEx.statusCode() == HttpStatus.SC_BAD_REQUEST;
    }
    return false;
  }

  /* Returns the material descriptor key */
  @Override
  public String getMatdescKey() {
    return "x-amz-matdesc";
  }

  /* Adds encryption metadata to the StorageObjectMetadata object */
  @Override
  public void addEncryptionMetadata(
          StorageObjectMetadata meta,
          MatDesc matDesc,
          byte[] ivData,
          byte[] encryptedKey,
          long contentLength) {
    meta.addUserMetadata(getMatdescKey(), matDesc.toString());
    meta.addUserMetadata(AMZ_KEY, Base64.getEncoder().encodeToString(encryptedKey));
    meta.addUserMetadata(AMZ_IV, Base64.getEncoder().encodeToString(ivData));
    meta.setContentLength(contentLength);
  }

  /* Adds digest metadata to the StorageObjectMetadata object */
  @Override
  public void addDigestMetadata(StorageObjectMetadata meta, String digest) {
    meta.addUserMetadata("sfc-digest", digest);
  }

  /* Gets digest metadata to the StorageObjectMetadata object */
  @Override
  public String getDigestMetadata(StorageObjectMetadata meta) {
    return meta.getUserMetadata().get("sfc-digest");
  }

  /*
   * Adds streaming ingest metadata to the StorageObjectMetadata object, used for streaming ingest
   * per client billing calculation
   */
  @Override
  public void addStreamingIngestMetadata(
          StorageObjectMetadata meta, String clientName, String clientKey) {
    meta.addUserMetadata(S3_STREAMING_INGEST_CLIENT_NAME, clientName);
    meta.addUserMetadata(S3_STREAMING_INGEST_CLIENT_KEY, clientKey);
  }

  @Override
  public String getStreamingIngestClientName(StorageObjectMetadata meta) {
    return meta.getUserMetadata().get(S3_STREAMING_INGEST_CLIENT_NAME);
  }

  @Override
  public String getStreamingIngestClientKey(StorageObjectMetadata meta) {
    return meta.getUserMetadata().get(S3_STREAMING_INGEST_CLIENT_KEY);
  }

  public static class ClientConfiguration {
    private final int maxConnections;
    private final int maxErrorRetry;
    private final int connectionTimeout;
    private final int socketTimeout;

    public ClientConfiguration(
            int maxConnections, int maxErrorRetry, int connectionTimeout, int socketTimeout) {
      this.maxConnections = maxConnections;
      this.maxErrorRetry = maxErrorRetry;
      this.connectionTimeout = connectionTimeout;
      this.socketTimeout = socketTimeout;
    }

    public int getMaxConnections() {
      return maxConnections;
    }

    public int getMaxErrorRetry() {
      return maxErrorRetry;
    }

    public int getConnectionTimeout() {
      return connectionTimeout;
    }

    public int getSocketTimeout() {
      return socketTimeout;
    }
  }
}
