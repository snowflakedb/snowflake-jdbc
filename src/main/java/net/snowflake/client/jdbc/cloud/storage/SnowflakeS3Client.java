/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

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
  private final  static String AMZ_MATDESC = "x-amz-matdesc";
  private final  static String AMZ_KEY = "x-amz-key";
  private final  static String AMZ_IV = "x-amz-iv";
  private final  static String FILE_CIPHER = "AES/CBC/PKCS5Padding";
  private final  static String KEY_CIPHER = "AES/ECB/PKCS5Padding";
  private final  static int BUFFER_SIZE = 2*1024*1024; // 2 MB

  // expired AWS token error code
  private final static String EXPIRED_AWS_TOKEN_ERROR_CODE = "ExpiredToken";

  private static SecureRandom secRnd;

  private int encryptionKeySize = 0; // used for PUTs
  private AmazonS3Client amazonClient = null;
  private RemoteStoreFileEncryptionMaterial encMat = null;
  private ClientConfiguration clientConfig = null;
  private String stageRegion = null;

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
    String awsID = (String)stageCredentials.get("AWS_ID");
    String awsKey = (String)stageCredentials.get("AWS_KEY");
    String awsToken = (String)stageCredentials.get("AWS_TOKEN");
    
    // initialize aws credentials
    AWSCredentials awsCredentials = (awsToken != null) ?
            new BasicSessionCredentials(awsID, awsKey, awsToken)
            : new BasicAWSCredentials(awsID, awsKey);


    clientConfig.withSignerOverride("AWSS3V4SignerType");
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
  }

  private static synchronized SecureRandom getSecRnd()
          throws NoSuchAlgorithmException,
                 NoSuchProviderException
  {
    if (secRnd == null)
    {
      secRnd = SecureRandom.getInstance("SHA1PRNG", "SUN");
      byte[] bytes = new byte[10];
      secRnd.nextBytes(bytes);
    }
    return secRnd;
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
            decrypt(localFile, key, iv);
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
    Pair<InputStream, Boolean> uploadStreamInfo =
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

        if (uploadStreamInfo.getRight())
        {
          myUpload = tx.upload(remoteStorageLocation, destFileName,
                  uploadStreamInfo.getLeft(), s3Meta);
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


  private void decrypt(File file, String keyBase64, String ivBase64)
          throws NoSuchAlgorithmException,
                 NoSuchPaddingException,
                 InvalidKeyException,
                 IllegalBlockSizeException,
                 BadPaddingException,
                 InvalidAlgorithmParameterException,
                 IOException
  {
    byte[] keyBytes = Base64.decode(keyBase64);
    byte[] ivBytes = Base64.decode(ivBase64);
    byte[] qsmkBytes = Base64.decode(encMat.getQueryStageMasterKey());
    final SecretKey fileKey;

    // Decrypt file key
    {
      final Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
      SecretKey queryStageMasterKey =
          new SecretKeySpec(qsmkBytes, 0, qsmkBytes.length, AES);
      keyCipher.init(Cipher.DECRYPT_MODE, queryStageMasterKey);
      byte[] fileKeyBytes = keyCipher.doFinal(keyBytes);

      // NB: we assume qsmk.length == fileKey.length
      //     (fileKeyBytes.length may be bigger due to padding)
      fileKey = new SecretKeySpec(fileKeyBytes, 0, qsmkBytes.length, AES);
    }



    // Decrypt file
    {
      final Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
      final IvParameterSpec iv = new IvParameterSpec(ivBytes);
      final byte[] buffer = new byte[BUFFER_SIZE];
      fileCipher.init(Cipher.DECRYPT_MODE, fileKey, iv);

      long totalBytesRead = 0;
      // Overwrite file contents buffer-wise with decrypted data
      try (InputStream is = Files.newInputStream(file.toPath(), READ);
           InputStream cis = new CipherInputStream(is, fileCipher);
           OutputStream os = Files.newOutputStream(file.toPath(), CREATE);)
      {
        int bytesRead;
        while ((bytesRead = cis.read(buffer)) > -1)
        {
          os.write(buffer, 0, bytesRead);
          totalBytesRead += bytesRead;
        }
      }

      // Discard any padding that the encrypted file had
      try (FileChannel fc = new FileOutputStream(file, true).getChannel())
      {
        fc.truncate(totalBytesRead);
      }
    }
  }

  private CipherInputStream encrypt(ObjectMetadata meta,
                                    long originalContentLength,
                                    InputStream src)
          throws InvalidKeyException,
                 InvalidAlgorithmParameterException,
                 NoSuchAlgorithmException,
                 NoSuchProviderException,
                 NoSuchPaddingException,
                 FileNotFoundException,
                 IllegalBlockSizeException,
                 BadPaddingException
  {
    final byte[] decodedKey = Base64.decode(encMat.getQueryStageMasterKey());
    final int keySize = decodedKey.length;
    final byte[] fileKeyBytes = new byte[keySize];
    final byte[] ivData;
    final CipherInputStream cis;
    final int blockSz;
    {
      final Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
      blockSz = fileCipher.getBlockSize();

      // Create IV
      ivData = new byte[blockSz];
      getSecRnd().nextBytes(ivData);
      final IvParameterSpec iv = new IvParameterSpec(ivData);

      // Create file key
      getSecRnd().nextBytes(fileKeyBytes);
      SecretKey fileKey = new SecretKeySpec(fileKeyBytes, 0, keySize, AES);



      // Init cipher
      fileCipher.init(Cipher.ENCRYPT_MODE, fileKey, iv);

      // Create encrypting input stream
      cis = new CipherInputStream(src, fileCipher);
    }


    // Encrypt the file key with the QRMK
    {
      final Cipher keyCipher =  Cipher.getInstance(KEY_CIPHER);
      SecretKey queryStageMasterKey =
          new SecretKeySpec(decodedKey, 0, keySize, AES);

      // Init cipher
      keyCipher.init(Cipher.ENCRYPT_MODE, queryStageMasterKey);
      byte[] encKeK = keyCipher.doFinal(fileKeyBytes);

      // Store metadata
      MatDesc matDesc =
          new MatDesc(encMat.getSmkId(), encMat.getQueryId(), keySize * 8);
      meta.addUserMetadata(AMZ_MATDESC,
                           matDesc.toString());
      meta.addUserMetadata(AMZ_KEY,
                           Base64.encodeAsString(encKeK));
      meta.addUserMetadata(AMZ_IV,
                           Base64.encodeAsString(ivData));
      // Round up length to next multiple of the block size
      // Sizes that are multiples of the block size need to be padded to next
      // multiple
      meta.setContentLength(
              ((originalContentLength + blockSz) / blockSz) * blockSz);

    }

    return cis;
  }

  private Pair<InputStream, Boolean>
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
          result = encrypt(meta, originalContentLength, uploadStream);
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
      return new ImmutablePair(result, uploadFromStream);
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

}