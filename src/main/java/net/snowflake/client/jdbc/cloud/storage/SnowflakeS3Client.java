/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.Base64;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSSLConnectionSocketFactory;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SFPair;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLInitializationException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Wrapper around AmazonS3Client.
 * @author ffunke
 */
public class SnowflakeS3Client implements SnowflakeStorageClient
{
  private final static SFLogger logger =
    SFLoggerFactory.getLogger(SnowflakeS3Client.class);
  private final  static String localFileSep =
      System.getProperty("file.separator");
  private final  static String AES = "AES";
  private final  static String AMZ_KEY = "x-amz-key";
  private final  static String AMZ_IV = "x-amz-iv";

  // expired AWS token error code
  private final static String EXPIRED_AWS_TOKEN_ERROR_CODE = "ExpiredToken";

  private int encryptionKeySize = 0; // used for PUTs
  private AmazonS3Client amazonClient = null;
  private RemoteStoreFileEncryptionMaterial encMat = null;
  private ClientConfiguration clientConfig = null;
  private String stageRegion = null;

  // socket factory used by s3 client's http client.
  private static SSLConnectionSocketFactory s3ConnectionSocketFactory = null;

  public SnowflakeS3Client(Map stageCredentials,
                           ClientConfiguration clientConfig,
                           RemoteStoreFileEncryptionMaterial encMat,
                           String stageRegion)
          throws SnowflakeSQLException
  {
      setupSnowflakeS3Client(stageCredentials, clientConfig,
                            encMat, stageRegion);
  }

    private void setupSnowflakeS3Client(Map stageCredentials,
                                        ClientConfiguration clientConfig,
                                        RemoteStoreFileEncryptionMaterial encMat,
                                        String stageRegion)
          throws SnowflakeSQLException
  {
    // Save the client creation parameters so that we can reuse them,
    // to reset the AWS client. We won't save the awsCredentials since
    // we will be refreshing that, every time we reset the AWS client
    this.clientConfig = clientConfig;
    this.stageRegion = stageRegion;
    this.encMat = encMat;

    logger.debug("Setting up AWS client ");

    // Retrieve S3 stage credentials
    String awsID = (String)stageCredentials.get("AWS_KEY_ID");
    String awsKey = (String)stageCredentials.get("AWS_SECRET_KEY");
    String awsToken = (String)stageCredentials.get("AWS_TOKEN");
    
    // initialize aws credentials
    AWSCredentials awsCredentials = (awsToken != null) ?
            new BasicSessionCredentials(awsID, awsKey, awsToken)
            : new BasicAWSCredentials(awsID, awsKey);


    clientConfig.withSignerOverride("AWSS3V4SignerType");
    clientConfig.getApacheHttpClientConfig().setSslSocketFactory(
        getSSLConnectionSocketFactory());
    if (encMat != null)
    {
      byte[] decodedKey = Base64.decode(encMat.getQueryStageMasterKey());
      encryptionKeySize = decodedKey.length*8;

      if (encryptionKeySize == 256)
      {
        SecretKey queryStageMasterKey =
            new SecretKeySpec(decodedKey, 0, decodedKey.length, AES);
        EncryptionMaterials encryptionMaterials =
            new EncryptionMaterials(queryStageMasterKey);
        encryptionMaterials.addDescription("queryId",
                                           encMat.getQueryId());
        encryptionMaterials.addDescription("smkId",
                                           Long.toString(encMat.getSmkId()));
        CryptoConfiguration cryptoConfig =
            new CryptoConfiguration(CryptoMode.EncryptionOnly);

        amazonClient = new AmazonS3EncryptionClient(awsCredentials,
              new StaticEncryptionMaterialsProvider(encryptionMaterials),
              clientConfig, cryptoConfig);
      }
      else if (encryptionKeySize == 128)
      {
        amazonClient = new AmazonS3Client(awsCredentials, clientConfig);
      }
      else
      {
        throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "unsupported key size", encryptionKeySize);
      }
    }
    else
    {
      amazonClient = new AmazonS3Client(awsCredentials, clientConfig);
    }

    if (stageRegion != null)
    {
      Region region = RegionUtils.getRegion(stageRegion);
      if (region != null)
      {
        amazonClient.setRegion(region);
      }
    }
    // Explicitly force to use virtual address style
    amazonClient.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(false).build());
  }

  // Returns the Max number of retry attempts
  @Override
  public int getMaxRetries()
  {
    return 25;
  }

  // Returns the max exponent for multiplying backoff with the power of 2, the value
  // of 4 will give us 16secs as the max number of time to sleep before retry
  @Override
  public int getRetryBackoffMaxExponent()
  {
    return 4;
  }

  // Returns the min number of milliseconds to sleep before retry
  @Override
  public int getRetryBackoffMin()
  {
    return 1000;
  }
  
  @Override
  public boolean isEncrypting()
  {
    return encryptionKeySize > 0;
  }

  @Override
  public int getEncryptionKeySize()
  {
    return encryptionKeySize;
  }

  /**
   * Renew the S3 client with fresh AWS credentials/access token
   * @param stageCredentials a Map of new AWS credential properties, to refresh the client with (as returned by GS)
   * @throws SnowflakeSQLException if any error occurs
   */
  @Override
  public void renew(Map stageCredentials)
          throws SnowflakeSQLException
  {
      // We renew the client with fresh credentials and with its original parameters
      setupSnowflakeS3Client(stageCredentials,this.clientConfig, this.encMat, this.stageRegion);
  }

  @Override
  public void shutdown()
  {
    amazonClient.shutdown();
  }

  @Override
  public StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix)
                          throws StorageProviderException
  {
    ObjectListing objListing = amazonClient.listObjects(remoteStorageLocation, prefix);

    return  new StorageObjectSummaryCollection(objListing.getObjectSummaries());
  }

  @Override
  public StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix)
                          throws StorageProviderException
  {
    return new S3ObjectMetadata(amazonClient.getObjectMetadata(remoteStorageLocation, prefix));
  }

  /**
   * Download a file from S3.
   * @param connection connection object
   * @param command command to download file
   * @param localLocation local file path
   * @param destFileName destination file name
   * @param parallelism number of threads for parallel downloading
   * @param remoteStorageLocation s3 bucket name
   * @param stageFilePath stage file path
   * @param stageRegion region name where the stage persists
   * @throws SnowflakeSQLException if download failed without an exception
   * @throws SnowflakeSQLException if failed to decrypt downloaded file
   * @throws SnowflakeSQLException if file metadata is incomplete
   */
  @Override
  public void download(       SFSession connection,
                              String command,
                              String localLocation,
                              String destFileName,
                              int parallelism,
                              String remoteStorageLocation,
                              String stageFilePath,
                              String stageRegion) throws SnowflakeSQLException
  {
    TransferManager tx = null;
    int retryCount = 0;
    do
    {
      try
      {
        File localFile = new File(localLocation + localFileSep + destFileName);

        logger.debug("Creating executor service for transfer" +
            "manager with {} threads", parallelism);

        // download file from s3
        tx = new TransferManager(amazonClient,
            SnowflakeUtil.createDefaultExecutorService(
                "s3-transfer-manager-downloader-", parallelism));

        Download myDownload = tx.download(remoteStorageLocation,
            stageFilePath, localFile);

        // Pull object metadata from S3
        ObjectMetadata meta =
            amazonClient.getObjectMetadata(remoteStorageLocation, stageFilePath);

        Map<String,String> metaMap = meta.getUserMetadata();
        String key = metaMap.get(AMZ_KEY);
        String iv = metaMap.get(AMZ_IV);

        myDownload.waitForCompletion();

        if (this.isEncrypting() && this.getEncryptionKeySize() < 256)
        {
          if (key == null || iv == null)
          {
            throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "File metadata incomplete");
          }

          // Decrypt file
          try
          {
            EncryptionProvider.decrypt(localFile, key, iv, this.encMat);
          }
          catch (Exception ex)
          {
            logger.error("Error decrypting file",ex);
            throw ex;
          }
        }

        return;

      } catch (Exception ex)
      {
        handleS3Exception(ex, ++retryCount, "download",
                connection, command, this);

      }
      finally
      {
        if (tx != null)
          tx.shutdownNow(false);
      }
    }
    while (retryCount <= getMaxRetries());

    throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        "Unexpected: download unsuccessful without exception!");
  }

  /**
   * Upload a file (-stream) to S3.
   * @param connection connection object
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
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  @Override
  public void upload(
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
          String stageRegion) throws SnowflakeSQLException
  {
    final long originalContentLength = meta.getContentLength();
    final List<FileInputStream> toClose = new ArrayList<>();
    SFPair<InputStream, Boolean> uploadStreamInfo =
            createUploadStream( srcFile, uploadFromStream,
                    inputStream, fileBackedOutputStream,
                    ((S3ObjectMetadata)meta).getS3ObjectMetadata(),
                    originalContentLength, toClose);

    ObjectMetadata s3Meta;
    if(meta instanceof S3ObjectMetadata) {
      s3Meta = ((S3ObjectMetadata) meta).getS3ObjectMetadata();
    }
    else throw new IllegalArgumentException("Unexpected metadata object type");

    TransferManager tx = null;
    int retryCount = 0;
    do
    {
      try
      {

        logger.debug("Creating executor service for transfer" +
                "manager with {} threads", parallelism);

        // upload files to s3
        tx = new TransferManager(this.amazonClient,
                SnowflakeUtil.createDefaultExecutorService(
                        "s3-transfer-manager-uploader-",
                        parallelism));

        final Upload myUpload;

        if (uploadStreamInfo.right)
        {
          myUpload = tx.upload(remoteStorageLocation, destFileName,
                  uploadStreamInfo.left, s3Meta);
        }
        else
        {
          PutObjectRequest putRequest =
                  new PutObjectRequest(remoteStorageLocation, destFileName, srcFile);
          putRequest.setMetadata(s3Meta);

          myUpload = tx.upload(putRequest);
        }

        myUpload.waitForCompletion();

        // get out
        for (FileInputStream is : toClose)
          IOUtils.closeQuietly(is);
        return;
      }
      catch (Exception ex)
      {

        handleS3Exception(ex, ++retryCount, "upload",
                connection, command, this);
        if (uploadFromStream && fileBackedOutputStream == null)
        {
          throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
                  ErrorCode.IO_ERROR.getMessageCode(),
                  "Encountered exception during upload: " +
                          ex.getMessage() + "\nCannot retry upload from stream.");
        }
        uploadStreamInfo = createUploadStream(srcFile, uploadFromStream,
                inputStream, fileBackedOutputStream,
                s3Meta, originalContentLength, toClose);
      } finally
      {
        if (tx != null)
          tx.shutdownNow(false);
      }
    }
    while(retryCount <= getMaxRetries());

    for (FileInputStream is : toClose)
      IOUtils.closeQuietly(is);

    throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            "Unexpected: upload unsuccessful without exception!");
  }

  private SFPair<InputStream, Boolean>
        createUploadStream( File srcFile,
                            boolean uploadFromStream,
                            InputStream inputStream,
                            FileBackedOutputStream fileBackedOutputStream,
                            ObjectMetadata meta,
                            long originalContentLength,
                            List<FileInputStream> toClose)
          throws SnowflakeSQLException
  {
      logger.debug(
                 "createUploadStream({}, {}, {}, {}, {}, {}, {}) "+
                 "keySize={}",
                 this,srcFile,uploadFromStream,inputStream,
                              fileBackedOutputStream, meta, toClose,
                              this.getEncryptionKeySize());
      final InputStream result;
      FileInputStream srcFileStream = null;
      if (isEncrypting() && getEncryptionKeySize() < 256)
      {
        try
        {
          final InputStream uploadStream = uploadFromStream ?
              (fileBackedOutputStream != null ?
                    fileBackedOutputStream.asByteSource().openStream() :
                    inputStream) :
              (srcFileStream = new FileInputStream(srcFile));
          toClose.add(srcFileStream);

          // Encrypt
          S3StorageObjectMetadata s3Metadata = new S3StorageObjectMetadata(meta);
          result = EncryptionProvider.encrypt(s3Metadata, originalContentLength,
                                              uploadStream, this.encMat, this);
          uploadFromStream = true;
        }
        catch (Exception ex)
        {
          logger.error("Failed to encrypt input", ex);
          throw new SnowflakeSQLException(ex, SqlState.INTERNAL_ERROR,
                  ErrorCode.INTERNAL_ERROR.getMessageCode(),
                  "Failed to encrypt input",ex.getMessage());
        }
      }
      else
      {
        try
        {
          result = uploadFromStream ?
                   (fileBackedOutputStream != null ?
               fileBackedOutputStream.asByteSource().openStream() :
               inputStream) :
                   (srcFileStream = new FileInputStream(srcFile));
          toClose.add(srcFileStream);

        }
        catch (FileNotFoundException ex)
        {
          logger.error("Failed to open input file", ex);
          throw new SnowflakeSQLException(ex, SqlState.INTERNAL_ERROR,
                  ErrorCode.INTERNAL_ERROR.getMessageCode(),
                  "Failed to open input file",ex.getMessage());
        }
        catch (IOException ex)
        {
          logger.error("Failed to open input stream", ex);
          throw new SnowflakeSQLException(ex, SqlState.INTERNAL_ERROR,
                  ErrorCode.INTERNAL_ERROR.getMessageCode(),
                  "Failed to open input stream",ex.getMessage());
        }
      }
      return SFPair.of(result, uploadFromStream);
  }


  @Override
  public void handleStorageException(Exception ex, int retryCount, String operation,SFSession connection, String command)
          throws SnowflakeSQLException
  {
      handleS3Exception(ex, retryCount, operation, connection, command, this);
  }

  private static void handleS3Exception(
                          Exception ex,
                          int retryCount,
                          String operation,
                          SFSession connection,
                          String command,
                          SnowflakeS3Client s3Client)
      throws SnowflakeSQLException
  {
    // no need to retry if it is invalid key exception
    if (ex.getCause() instanceof InvalidKeyException)
    {
      // Most likely cause is that the unlimited strength policy files are not installed
      // Log the error and throw a message that explains the cause
      SnowflakeFileTransferAgent.throwJCEMissingError(operation, ex);
    }

    if (ex instanceof AmazonClientException)
    {
      if (retryCount > s3Client.getMaxRetries())
      {
        String extendedRequestId = "none";

        if (ex instanceof AmazonS3Exception)
        {
          AmazonS3Exception ex1 = (AmazonS3Exception) ex;
          extendedRequestId = ex1.getExtendedRequestId();
        }

        if (ex instanceof AmazonServiceException)
        {
          AmazonServiceException ex1 = (AmazonServiceException) ex;
          throw new SnowflakeSQLException(ex1, SqlState.SYSTEM_ERROR,
              ErrorCode.S3_OPERATION_ERROR.getMessageCode(),
              operation,
              ex1.getErrorType().toString(),
              ex1.getErrorCode(),
              ex1.getMessage(), ex1.getRequestId(),
              extendedRequestId);
        }
        else
          throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
              ErrorCode.AWS_CLIENT_ERROR.getMessageCode(),
              operation, ex.getMessage());
      }
      else {
        logger.debug("Encountered exception ({}) during {}, retry count: {}",
                ex.getMessage(), operation, retryCount);
        logger.debug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis = s3Client.getRetryBackoffMin();

        if (retryCount > 1)
          backoffInMillis <<= (Math.min(retryCount - 1,
                  s3Client.getRetryBackoffMaxExponent()));

        try {
          logger.debug("Sleep for {} milliseconds before retry",
                  backoffInMillis);

          Thread.sleep(backoffInMillis);
        } catch (InterruptedException ex1) {
          // ignore
        }

        // If the exception indicates that the AWS token has expired,
        // we need to refresh our S3 client with the new token
        if (ex instanceof AmazonS3Exception)
        {
          AmazonS3Exception s3ex = (AmazonS3Exception) ex;
          if (s3ex.getErrorCode().equalsIgnoreCase(EXPIRED_AWS_TOKEN_ERROR_CODE))
          {
            SnowflakeFileTransferAgent.renewExpiredToken(connection, command, s3Client);
          }
        }
      }
    }
    else
    {
      if (ex instanceof InterruptedException ||
          SnowflakeUtil.getRootCause(ex) instanceof SocketTimeoutException)
      {
        if (retryCount > s3Client.getMaxRetries())
          throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
              ErrorCode.IO_ERROR.getMessageCode(),
              "Encountered exception during " + operation +  ": " +
                  ex.getMessage());
        else
        {
          logger.debug("Encountered exception ({}) during {}, retry count: {}",
              ex.getMessage(), operation, retryCount);
        }
      }
      else
        throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
            ErrorCode.IO_ERROR.getMessageCode(),
            "Encountered exception during " + operation + ": " +
                ex.getMessage());
    }
  }

  /**
   * Returns the material descriptor key
   */
  @Override
  public String getMatdescKey()
  {
    return "x-amz-matdesc";
  }

  /**
   * Adds encryption metadata to the StorageObjectMetadata object
   */
   @Override
   public void addEncryptionMetadata(StorageObjectMetadata meta,
                              MatDesc matDesc,
                              byte[] ivData,
                              byte[] encKeK,
                              long contentLength)
   {
      meta.addUserMetadata(getMatdescKey(),
                           matDesc.toString());
      meta.addUserMetadata(AMZ_KEY,
                           Base64.encodeAsString(encKeK));
      meta.addUserMetadata(AMZ_IV,
                           Base64.encodeAsString(ivData));
      meta.setContentLength(contentLength);
   }

  /**
   * Adds digest metadata to the StorageObjectMetadata object
   */
   @Override
   public void addDigestMetadata(StorageObjectMetadata meta, String digest)
   {
     meta.addUserMetadata("sfc-digest", digest);
   }

  /**
   * Gets digest metadata to the StorageObjectMetadata object
   */
   @Override
   public String getDigestMetadata(StorageObjectMetadata meta)
   {
     return meta.getUserMetadata().get("sfc-digest");
   }

  private static SSLConnectionSocketFactory getSSLConnectionSocketFactory()
  {
    if (s3ConnectionSocketFactory == null)
    {
      synchronized (SnowflakeS3Client.class)
      {
        if (s3ConnectionSocketFactory == null)
        {
          try
          {
            // trust manager is set to null, which will use default ones
            // instead of SFTrustManager (which enables ocsp checking)
            s3ConnectionSocketFactory = new SFSSLConnectionSocketFactory(null,
                HttpUtil.isSocksProxyDisabled());
          }
          catch (KeyManagementException | NoSuchAlgorithmException e)
          {
            throw new SSLInitializationException(e.getMessage(), e);
          }
        }
      }
    }

    return s3ConnectionSocketFactory;
  }
}