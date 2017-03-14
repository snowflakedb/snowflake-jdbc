/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
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
import net.snowflake.common.core.S3FileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
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
import java.security.DigestOutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

/**
 * Wrapper around AmazonS3Client.
 * @author ffunke
 */
public class SnowflakeS3Client
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


  private static SecureRandom secRnd;


  private int encryptionKeySize = 0; // used for PUTs
  private AmazonS3Client amazonClient = null;
  private S3FileEncryptionMaterial encMat = null;

  public SnowflakeS3Client(AWSCredentials awsCredentials,
                           ClientConfiguration clientConfig,
                           S3FileEncryptionMaterial encMat,
                           String stageRegion)
         throws SnowflakeSQLException
  {
    this.encMat = encMat;
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

  public boolean isEncrypting()
  {
    return encryptionKeySize > 0;
  }

  public int getEncryptionKeySize()
  {
    return encryptionKeySize;
  }

  public void shutdown()
  {
    amazonClient.shutdown();
  }

  public ObjectListing listObjects(String bucketName, String prefix)
                          throws AmazonClientException,
                                 AmazonServiceException
  {
    return amazonClient.listObjects(bucketName, prefix);
  }

  public ObjectMetadata getObjectMetadata(String bucketName, String prefix)
                          throws AmazonClientException,
                                 AmazonServiceException
  {
    return amazonClient.getObjectMetadata(bucketName, prefix);
  }

  /**
   * Download a file from S3.
   * @param client s3 client instance
   * @param connection connection object
   * @param command command to download file
   * @param localLocation local file path
   * @param destFileName destination file name
   * @param parallelism number of threads for parallel downloading
   * @param bucketName s3 bucket name
   * @param stageFilePath stage file path
   * @throws SnowflakeSQLException if download failed without an exception
   * @throws SnowflakeSQLException if failed to decrypt downloaded file
   * @throws SnowflakeSQLException if file metadata is incomplete
   */
  public static void download(SnowflakeS3Client client,
                              SFSession connection,
                              String command,
                              String localLocation,
                              String destFileName,
                              int parallelism,
                              String bucketName,
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
        tx = new TransferManager(client.amazonClient,
            SnowflakeUtil.createDefaultExecutorService(
                "s3-transfer-manager-downloader-", parallelism));

        Download myDownload = tx.download(bucketName,
            stageFilePath, localFile);

        // Pull object metadata from S3
        ObjectMetadata meta =
            client.amazonClient.getObjectMetadata(bucketName, stageFilePath);

        Map<String,String> metaMap = meta.getUserMetadata();
        String key = metaMap.get(AMZ_KEY);
        String iv = metaMap.get(AMZ_IV);

        myDownload.waitForCompletion();

        if (client.isEncrypting() && client.getEncryptionKeySize() < 256)
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
            client.decrypt(localFile, key, iv);
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
        client = handleS3Exception(ex, ++retryCount, "download", client.encMat,
            connection, command, parallelism, client, stageRegion);
      }
      finally
      {
        if (tx != null)
          tx.shutdownNow(false);
      }
    }
    while (retryCount <= SnowflakeFileTransferAgent.CLIENT_SIDE_MAX_RETRIES);

    throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        "Unexpected: download unsuccessful without exception!");
  }

  private void decrypt(File file, String keyBase64, String ivBase64)
          throws NoSuchAlgorithmException,
                 NoSuchPaddingException,
                 InvalidKeyException,
                 IllegalBlockSizeException,
                 BadPaddingException,
                 InvalidAlgorithmParameterException,
                 FileNotFoundException,
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

  private static Pair<InputStream, Boolean>
        createUploadStream(SnowflakeS3Client client,
      File srcFile,
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
                 client,srcFile,uploadFromStream,inputStream,
                              fileBackedOutputStream, meta, toClose,
                              client.getEncryptionKeySize());
      final InputStream result;
      FileInputStream srcFileStream = null;
      if (client.isEncrypting() && client.getEncryptionKeySize() < 256)
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
          result = client.encrypt(meta, originalContentLength, uploadStream);
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

  /**
   * Upload a file (-stream) to S3.
   * @param client client object used to communicate with s3
   * @param connection connection object
   * @param command upload command
   * @param parallelism number of threads do parallel uploading
   * @param maxRetries max number of retries if upload failed
   * @param uploadFromStream true if upload source is stream
   * @param bucketName s3 bucket name
   * @param srcFile source file if not uploading from a stream
   * @param destFileName file name on s3 after upload
   * @param inputStream stream used for uploading if fileBackedOutputStream is null
   * @param fileBackedOutputStream stream used for uploading if not null
   * @param meta object meta data
   * @throws SnowflakeSQLException if upload failed even after retry
   */
  public static void upload(SnowflakeS3Client client,
                            SFSession connection,
                            String command,
                            int parallelism,
                            int maxRetries,
                            boolean uploadFromStream,
                            String bucketName,
                            File srcFile,
                            String destFileName,
                            InputStream inputStream,
                            FileBackedOutputStream fileBackedOutputStream,
                            ObjectMetadata meta,
                            String stageRegion) throws SnowflakeSQLException
  {
    final long originalContentLength = meta.getContentLength();
    final List<FileInputStream> toClose = new ArrayList<>();
    Pair<InputStream, Boolean> uploadStreamInfo =
        createUploadStream(client, srcFile, uploadFromStream,
                           inputStream, fileBackedOutputStream,
                           meta, originalContentLength, toClose);
    TransferManager tx = null;
    int retryCount = 0;
    do
    {
      try
      {
        logger.debug("Creating executor service for transfer" +
            "manager with {} threads", parallelism);

        // upload files to s3
        tx = new TransferManager(client.amazonClient,
            SnowflakeUtil.createDefaultExecutorService(
                "s3-transfer-manager-uploader-",
                parallelism));

        final Upload myUpload;

        if (uploadStreamInfo.getRight())
        {
          myUpload = tx.upload(bucketName, destFileName,
              uploadStreamInfo.getLeft(), meta);
        }
        else
        {
          PutObjectRequest putRequest =
              new PutObjectRequest(bucketName, destFileName, srcFile);
          putRequest.setMetadata(meta);

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
        client = handleS3Exception(ex, ++retryCount, "upload", client.encMat,
            connection, command, parallelism, client, stageRegion);
        if (uploadFromStream && fileBackedOutputStream == null)
        {
          throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
              ErrorCode.IO_ERROR.getMessageCode(),
              "Encountered exception during upload: " +
                  ex.getMessage() + "\nCannot retry upload from stream.");
        }
        uploadStreamInfo = createUploadStream(client, srcFile, uploadFromStream,
                           inputStream, fileBackedOutputStream,
                           meta, originalContentLength, toClose);
      } finally
      {
        if (tx != null)
          tx.shutdownNow(false);
      }
    }
    while(retryCount <= maxRetries);

    for (FileInputStream is : toClose)
      IOUtils.closeQuietly(is);

    throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        "Unexpected: upload unsuccessful without exception!");
  }

  private static SnowflakeS3Client handleS3Exception(Exception ex,
                                 int retryCount,
                                 String operation,
                                 S3FileEncryptionMaterial encMat,
                                 SFSession connection,
                                 String command,
                                 int parallel,
                                 SnowflakeS3Client s3Client,
                                 String stageRegion)
      throws SnowflakeSQLException
  {
    // no need to retry if it is invalid key exception
    if (ex.getCause() instanceof InvalidKeyException)
    {
      // Most likely cause: Unlimited strength policy files not installed
      String msg = "Strong encryption with Java JRE requires JCE " +
          "Unlimited Strength Jurisdiction Policy files. " +
          "Follow JDBC client installation instructions " +
          "provided by Snowflake or contact Snowflake Support.";
      logger.error(
          "JCE Unlimited Strength policy files missing: {}. {}.",
          ex.getMessage(), ex.getCause().getMessage());
      String bootLib =
          java.lang.System.getProperty("sun.boot.library.path");
      if (bootLib != null)
      {
        msg += " The target directory on your system is: " +
            Paths.get(bootLib,"security").toString();
        logger.error(msg);
      }
      throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
          ErrorCode.AWS_CLIENT_ERROR.getMessageCode(),
          operation, msg);
    }

    if (ex instanceof AmazonClientException)
    {
      if (retryCount > SnowflakeFileTransferAgent.CLIENT_SIDE_MAX_RETRIES)
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
      else
      {
        logger.debug("Encountered exception ({}) during {}, retry count: {}",
                    ex.getMessage(), operation, retryCount);
        logger.debug("Stack trace: ", ex);

        // exponential backoff up to a limit
        int backoffInMillis =
            SnowflakeFileTransferAgent.CLIENT_SIDE_RETRY_BACKOFF_MIN;
        if (retryCount > 1)
          backoffInMillis <<= (Math.min(retryCount-1,
              SnowflakeFileTransferAgent.CLIENT_SIDE_RETRY_BACKOFF_MAX_EXPONENT));

        try
        {
          logger.debug("Sleep for {} milliseconds before retry",
              backoffInMillis);

          Thread.sleep(backoffInMillis);
        }
        catch(InterruptedException ex1)
        {
          // ignore
        }

        return SnowflakeFileTransferAgent.renewExpiredAWSToken(
                connection, command, parallel, s3Client,
                (AmazonClientException) ex, encMat, stageRegion);
      }
    }
    else
    {
      if (ex instanceof InterruptedException ||
          SnowflakeUtil.getRootCause(ex) instanceof SocketTimeoutException)
      {
        if (retryCount > SnowflakeFileTransferAgent.CLIENT_SIDE_MAX_RETRIES)
          throw new SnowflakeSQLException(ex, SqlState.SYSTEM_ERROR,
              ErrorCode.IO_ERROR.getMessageCode(),
              "Encountered exception during " + operation +  ": " +
                  ex.getMessage());
        else
        {
          logger.debug("Encountered exception ({}) during {}, retry count: {}",
              ex.getMessage(), operation, retryCount);

          return s3Client;
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