package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.createDefaultExecutorService;
import static net.snowflake.client.internal.jdbc.cloud.storage.SnowflakeS3Client.EXPIRED_AWS_TOKEN_ERROR_CODE;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.api.http.HttpHeadersCustomizer;
import net.snowflake.client.internal.core.HeaderCustomizerHttpRequestInterceptor;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.client.internal.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.client.internal.util.SFPair;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

class GCSAccessStrategyAwsSdk implements GCSAccessStrategy {
  private static final SFLogger logger = SFLoggerFactory.getLogger(GCSAccessStrategyAwsSdk.class);
  private final S3AsyncClient amazonClient;

  GCSAccessStrategyAwsSdk(StageInfo stage, SFBaseSession session) throws SnowflakeSQLException {
    String accessToken = (String) stage.getCredentials().get("GCS_ACCESS_TOKEN");

    Optional<String> oEndpoint = stage.gcsCustomEndpoint();
    String endpoint = "https://storage.googleapis.com";
    if (oEndpoint.isPresent()) {
      endpoint = oEndpoint.get();
    }
    if (stage.getStorageAccount() != null && endpoint.startsWith(stage.getStorageAccount())) {
      endpoint = endpoint.replaceFirst(stage.getStorageAccount() + ".", "");
    }
    S3AsyncClientBuilder clientBuilder;
    try {
      clientBuilder =
          S3AsyncClient.builder()
              .region(Region.US_WEST_2) // dummy region, just to satisfy the builder
              .forcePathStyle(false)
              .endpointOverride(new URI(endpoint));
    } catch (URISyntaxException e) {
      throw new SnowflakeSQLException(
          ErrorCode.FILE_TRANSFER_ERROR, "Could not parse Google storage endpoint: " + endpoint);
    }

    ClientOverrideConfiguration.Builder overrideConfiguration =
        ClientOverrideConfiguration.builder();

    // Add signer interceptor for bearer token auth and header mapping
    overrideConfiguration.putAdvancedOption(
        SdkAdvancedClientOption.SIGNER, new AwsSdkGCPSigner(accessToken));

    ProxyConfiguration proxyConfiguration;
    if (session != null) {
      proxyConfiguration = S3HttpUtil.createProxyConfigurationForS3(session.getHttpClientKey());
    } else {
      proxyConfiguration =
          S3HttpUtil.createSessionlessProxyConfigurationForS3(stage.getProxyProperties());
    }

    if (session instanceof SFSession) {
      List<HttpHeadersCustomizer> headersCustomizers =
          ((SFSession) session).getHttpHeadersCustomizers();
      if (headersCustomizers != null && !headersCustomizers.isEmpty()) {
        overrideConfiguration.addExecutionInterceptor(
            new HeaderCustomizerHttpRequestInterceptor(headersCustomizers));
      }
    }

    clientBuilder.overrideConfiguration(overrideConfiguration.build());

    // Use anonymous credentials to minimize AWS signing
    clientBuilder.credentialsProvider(AnonymousCredentialsProvider.create());

    clientBuilder.httpClientBuilder(
        NettyNioAsyncHttpClient.builder().proxyConfiguration(proxyConfiguration));

    amazonClient = clientBuilder.build();
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
  public StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix) {
    HeadObjectResponse response =
        amazonClient
            .headObject(
                HeadObjectRequest.builder().bucket(remoteStorageLocation).key(prefix).build())
            .join();

    S3ObjectMetadata metadata = new S3ObjectMetadata(response);

    Map<String, String> userMetadata =
        response.metadata().entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("x-goog-meta-"))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().replaceFirst("x-goog-meta-", ""), Map.Entry::getValue));

    metadata.setUserMetadata(userMetadata);
    return metadata;
  }

  @Override
  public Map<String, String> download(
      int parallelism, String remoteStorageLocation, String stageFilePath, File localFile)
      throws InterruptedException {

    logger.debug(
        "Staring download of file from S3 stage path: {} to {}",
        stageFilePath,
        localFile.getAbsolutePath());

    logger.debug("Creating executor service for transfer manager with {} threads", parallelism);
    try (S3TransferManager tx =
        S3TransferManager.builder()
            .s3Client(amazonClient)
            .executor(createDefaultExecutorService("s3-transfer-manager-downloader-", parallelism))
            .build()) {
      // download files from s3

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
      StorageObjectMetadata meta = this.getObjectMetadata(remoteStorageLocation, stageFilePath);

      Map<String, String> metaMap = SnowflakeUtil.createCaseInsensitiveMap(meta.getUserMetadata());
      fileDownload.completionFuture().join();
      return metaMap;
    }
  }

  @Override
  public SFPair<InputStream, Map<String, String>> downloadToStream(
      String remoteStorageLocation, String stageFilePath, boolean isEncrypting) {
    InputStream stream =
        amazonClient
            .getObject(
                GetObjectRequest.builder().bucket(remoteStorageLocation).key(stageFilePath).build(),
                AsyncResponseTransformer.toBlockingInputStream())
            .join();
    HeadObjectResponse meta =
        amazonClient
            .headObject(
                HeadObjectRequest.builder()
                    .bucket(remoteStorageLocation)
                    .key(stageFilePath)
                    .build())
            .join();

    Map<String, String> metaMap = SnowflakeUtil.createCaseInsensitiveMap(meta.metadata());

    return SFPair.of(stream, metaMap);
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
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setContentEncoding(contentEncoding);
    s3ObjectMetadata.setContentLength(contentLength);
    s3ObjectMetadata.setUserMetadata(metadata);

    PutObjectRequest request =
        (s3ObjectMetadata)
            .getS3PutObjectRequest().toBuilder()
                .bucket(remoteStorageLocation)
                .key(destFileName)
                .build();

    logger.debug("Creating executor service for transfer manager with {} threads", parallelism);
    ThreadPoolExecutor executorService =
        createDefaultExecutorService("s3-transfer-manager-uploader-", parallelism);
    try (S3TransferManager tx =
        S3TransferManager.builder().s3Client(amazonClient).executor(executorService).build()) {
      // upload files to s3
      final Upload upload =
          tx.upload(
              UploadRequest.builder()
                  .putObjectRequest(request)
                  .requestBody(
                      AsyncRequestBody.fromInputStream(
                          // wrapping with BufferedInputStream to mitigate
                          // https://github.com/aws/aws-sdk-java-v2/issues/6174
                          new BufferedInputStream(content),
                          request.contentLength(),
                          executorService))
                  .build());
      upload.completionFuture().join();

      logger.info("Uploaded data from input stream to S3 location: {}.", destFileName);
    }
  }

  private static boolean isClientException400Or404(Throwable ex) {
    if (ex instanceof SdkServiceException) {
      SdkServiceException asEx = (SdkServiceException) (ex);
      return asEx.statusCode() == HttpStatus.SC_NOT_FOUND
          || asEx.statusCode() == HttpStatus.SC_BAD_REQUEST;
    }
    return false;
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
    Throwable cause = ex.getCause();
    if (cause instanceof SdkException) {
      logger.debug("GCSAccessStrategyAwsSdk: " + cause.getMessage());

      if (retryCount > gcsClient.getMaxRetries() || isClientException400Or404(cause)) {
        String extendedRequestId = "none";

        if (cause instanceof S3Exception) {
          S3Exception ex1 = (S3Exception) cause;
          extendedRequestId = ex1.extendedRequestId();
        }

        if (cause instanceof SdkServiceException) {
          SdkServiceException ex1 = (SdkServiceException) cause;

          // The AWS credentials might have expired when server returns error 400 and
          // does not return the ExpiredToken error code.
          // If session is null we cannot renew the token so throw the exception
          if (ex1.statusCode() == HttpStatus.SC_BAD_REQUEST && session != null) {
            SnowflakeFileTransferAgent.renewExpiredToken(session, command, gcsClient);
          } else {
            throw new SnowflakeSQLLoggedException(
                queryId,
                session,
                SqlState.SYSTEM_ERROR,
                StorageHelper.getOperationException(operation).getMessageCode(),
                ex1,
                operation,
                ex1.getMessage(),
                ex1.requestId(),
                extendedRequestId);
          }
        } else {
          throw new SnowflakeSQLLoggedException(
              queryId,
              session,
              SqlState.SYSTEM_ERROR,
              StorageHelper.getOperationException(operation).getMessageCode(),
              cause,
              operation,
              cause.getMessage());
        }
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

        // If the exception indicates that the AWS token has expired,
        // we need to refresh our S3 client with the new token
        if (cause instanceof S3Exception) {
          S3Exception s3ex = (S3Exception) cause;
          if (s3ex.awsErrorDetails().errorCode().equalsIgnoreCase(EXPIRED_AWS_TOKEN_ERROR_CODE)) {
            // If session is null we cannot renew the token so throw the ExpiredToken exception
            if (session != null) {
              SnowflakeFileTransferAgent.renewExpiredToken(session, command, gcsClient);
            } else {
              throw new SnowflakeSQLException(
                  queryId,
                  s3ex.awsErrorDetails().errorCode(),
                  CLOUD_STORAGE_CREDENTIALS_EXPIRED,
                  "S3 credentials have expired");
            }
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
    if (this.amazonClient != null) {
      this.amazonClient.close();
    }
  }
}
