/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static net.snowflake.client.core.Constants.NO_SPACE_LEFT_ON_DEVICE_ERR;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.FileUtil;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFFixedViewResultSet;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeStorageClient;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageClientFactory;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectSummary;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectSummaryCollection;
import net.snowflake.client.jdbc.cloud.storage.StorageProviderException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.FileCompressionType;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/**
 * Class for uploading/downloading files
 *
 * @author jhuang
 */
public class SnowflakeFileTransferAgent extends SFBaseFileTransferAgent {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeFileTransferAgent.class);

  static final StorageClientFactory storageFactory = StorageClientFactory.getFactory();

  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  // We will allow buffering of upto 128M data before spilling to disk during
  // compression and digest computation
  static final int MAX_BUFFER_SIZE = 1 << 27;
  public static final String SRC_FILE_NAME_FOR_STREAM = "stream";

  private static final String FILE_PROTOCOL = "file://";

  private static final String localFSFileSep = systemGetProperty("file.separator");
  private static final int DEFAULT_PARALLEL = 10;

  private final String command;

  // list of files specified. Wildcard should be expanded already for uploading
  // For downloading, it the list of stage file names
  private Set<String> sourceFiles;

  // big source files >=16MB, for which we will not upload them in serial mode
  // since TransferManager will parallelize upload
  private Set<String> bigSourceFiles;

  // big source files < 16MB, for which we will upload them in parallel mode
  // with 4 threads by default
  private Set<String> smallSourceFiles;

  // Threshold for splitting a file to upload multiple parts in parallel
  private int bigFileThreshold = 200 * 1024 * 1024;

  private Map<String, FileMetadata> fileMetadataMap;

  // stage related info
  private StageInfo stageInfo;

  // local location for where to download files to
  private String localLocation;

  // Query ID of PUT or GET statement
  private String queryID = null;

  // default parallelism
  private int parallel = DEFAULT_PARALLEL;
  private SFSession session;
  private SFStatement statement;
  private static Throwable injectedFileTransferException = null; // for testing purpose

  // This function should only be used for testing purpose
  static void setInjectedFileTransferException(Throwable th) {
    injectedFileTransferException = th;
  }

  static boolean isInjectedFileTransferExceptionEnabled() {
    return injectedFileTransferException != null;
  }

  public StageInfo getStageInfo() {
    return this.stageInfo;
  }

  /**
   * Get value of big file threshold. For testing purposes.
   *
   * @return integer value in bytes of threshold
   */
  int getBigFileThreshold() {
    return this.bigFileThreshold;
  }

  // Encryption material
  private List<RemoteStoreFileEncryptionMaterial> encryptionMaterial;

  // Presigned URLs
  private List<String> presignedUrls;

  // Index: Source file to encryption material
  HashMap<String, RemoteStoreFileEncryptionMaterial> srcFileToEncMat;

  // Index: Source file to presigned URL
  HashMap<String, String> srcFileToPresignedUrl;

  public Map<?, ?> getStageCredentials() {
    return new HashMap<>(stageInfo.getCredentials());
  }

  public List<RemoteStoreFileEncryptionMaterial> getEncryptionMaterial() {
    return new ArrayList<>(encryptionMaterial);
  }

  public Map<String, RemoteStoreFileEncryptionMaterial> getSrcToMaterialsMap() {
    return new HashMap<>(srcFileToEncMat);
  }

  public Map<String, String> getSrcToPresignedUrlMap() {
    return new HashMap<>(srcFileToPresignedUrl);
  }

  public String getStageLocation() {
    return stageInfo.getLocation();
  }

  private void initEncryptionMaterial(CommandType commandType, JsonNode jsonNode)
      throws SnowflakeSQLException, JsonProcessingException {
    encryptionMaterial = getEncryptionMaterial(commandType, jsonNode);
  }

  /**
   * Get the encryption information for an UPLOAD or DOWNLOAD given a PUT command response JsonNode
   *
   * @param commandType CommandType of action (e.g UPLOAD or DOWNLOAD)
   * @param jsonNode JsonNod of PUT call response
   * @return List of RemoteStoreFileEncryptionMaterial objects
   */
  static List<RemoteStoreFileEncryptionMaterial> getEncryptionMaterial(
      CommandType commandType, JsonNode jsonNode)
      throws SnowflakeSQLException, JsonProcessingException {
    List<RemoteStoreFileEncryptionMaterial> encryptionMaterial = new ArrayList<>();
    JsonNode rootNode = jsonNode.path("data").path("encryptionMaterial");
    if (commandType == CommandType.UPLOAD) {
      logger.debug("InitEncryptionMaterial: UPLOAD", false);

      RemoteStoreFileEncryptionMaterial encMat = null;
      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        encMat = mapper.treeToValue(rootNode, RemoteStoreFileEncryptionMaterial.class);
      }
      encryptionMaterial.add(encMat);

    } else {
      logger.debug("InitEncryptionMaterial: DOWNLOAD", false);

      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        encryptionMaterial =
            Arrays.asList(mapper.treeToValue(rootNode, RemoteStoreFileEncryptionMaterial[].class));
      }
    }
    return encryptionMaterial;
  }

  private void initPresignedUrls(CommandType commandType, JsonNode jsonNode)
      throws SnowflakeSQLException, JsonProcessingException, IOException {
    presignedUrls = getPresignedUrls(commandType, jsonNode);
  }

  private static List<String> getPresignedUrls(CommandType commandType, JsonNode jsonNode)
      throws SnowflakeSQLException, JsonProcessingException, IOException {
    List<String> presignedUrls = new ArrayList<>();
    JsonNode rootNode = jsonNode.path("data").path("presignedUrls");
    if (commandType == CommandType.DOWNLOAD) {
      logger.debug("InitEncryptionMaterial: DOWNLOAD", false);

      if (!rootNode.isMissingNode() && !rootNode.isNull()) {
        presignedUrls = Arrays.asList(mapper.readValue(rootNode.toString(), String[].class));
      }
    }
    return presignedUrls;
  }

  private boolean autoCompress = true;

  private boolean overwrite = false;

  private SnowflakeStorageClient storageClient = null;

  private static final String SOURCE_COMPRESSION_AUTO_DETECT = "auto_detect";
  private static final String SOURCE_COMPRESSION_NONE = "none";

  private String sourceCompression = SOURCE_COMPRESSION_AUTO_DETECT;

  private ExecutorService threadExecutor = null;
  private Boolean canceled = false;

  /** Result status enum */
  public enum ResultStatus {
    UNKNOWN("Unknown status"),
    UPLOADED("File uploaded"),
    UNSUPPORTED("File type not supported"),
    ERROR("Error encountered"),
    SKIPPED("Skipped since file exists"),
    NONEXIST("File does not exist"),
    COLLISION("File name collides with another file"),
    DIRECTORY("Not a file, but directory"),
    DOWNLOADED("File downloaded");

    private String desc;

    public String getDesc() {
      return desc;
    }

    private ResultStatus(String desc) {
      this.desc = desc;
    }
  }

  /** Remote object location location: "bucket" for S3, "container" for Azure BLOB */
  private static class remoteLocation {
    String location;
    String path;

    public remoteLocation(String remoteStorageLocation, String remotePath) {
      location = remoteStorageLocation;
      path = remotePath;
    }
  }

  /**
   * File metadata with everything we care so we don't need to repeat same processing to get these
   * info.
   */
  private class FileMetadata {
    public String srcFileName;
    public long srcFileSize;
    public String destFileName;
    public long destFileSize;
    public boolean requireCompress;
    public ResultStatus resultStatus = ResultStatus.UNKNOWN;
    public String errorDetails = "";
    public FileCompressionType srcCompressionType;
    public FileCompressionType destCompressionType;
    public boolean isEncrypted = false;
  }

  static class InputStreamWithMetadata {
    long size;
    String digest;

    // FileBackedOutputStream that should be destroyed when
    // the input stream has been consumed entirely
    FileBackedOutputStream fileBackedOutputStream;

    InputStreamWithMetadata(
        long size, String digest, FileBackedOutputStream fileBackedOutputStream) {
      this.size = size;
      this.digest = digest;
      this.fileBackedOutputStream = fileBackedOutputStream;
    }
  }

  /**
   * Compress an input stream with GZIP and return the result size, digest and compressed stream.
   *
   * @param inputStream data input
   * @param session the session
   * @return result size, digest and compressed stream
   * @throws SnowflakeSQLException if encountered exception when compressing
   */
  private static InputStreamWithMetadata compressStreamWithGZIP(
      InputStream inputStream, SFBaseSession session, String queryId) throws SnowflakeSQLException {
    FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

    try {

      DigestOutputStream digestStream =
          new DigestOutputStream(tempStream, MessageDigest.getInstance("SHA-256"));

      CountingOutputStream countingStream = new CountingOutputStream(digestStream);

      // construct a gzip stream with sync_flush mode
      GZIPOutputStream gzipStream;

      gzipStream = new GZIPOutputStream(countingStream, true);

      IOUtils.copy(inputStream, gzipStream);

      inputStream.close();

      gzipStream.finish();
      gzipStream.flush();

      countingStream.flush();

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()
          && SnowflakeFileTransferAgent.injectedFileTransferException
              instanceof NoSuchAlgorithmException) {
        throw (NoSuchAlgorithmException) SnowflakeFileTransferAgent.injectedFileTransferException;
      }

      return new InputStreamWithMetadata(
          countingStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          tempStream);

    } catch (IOException | NoSuchAlgorithmException ex) {
      logger.error("Exception compressing input stream", ex);

      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "error encountered for compression");
    }
  }

  /**
   * Compress an input stream with GZIP and return the result size, digest and compressed stream.
   *
   * @param inputStream The input stream to compress
   * @return the compressed stream
   * @throws SnowflakeSQLException Will be thrown if there is a problem with compression
   * @deprecated Can be removed when all accounts are encrypted
   */
  @Deprecated
  private static InputStreamWithMetadata compressStreamWithGZIPNoDigest(
      InputStream inputStream, SFBaseSession session, String queryId) throws SnowflakeSQLException {
    try {
      FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

      CountingOutputStream countingStream = new CountingOutputStream(tempStream);

      // construct a gzip stream with sync_flush mode
      GZIPOutputStream gzipStream;

      gzipStream = new GZIPOutputStream(countingStream, true);

      IOUtils.copy(inputStream, gzipStream);

      inputStream.close();

      gzipStream.finish();
      gzipStream.flush();

      countingStream.flush();

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()) {
        throw (IOException) SnowflakeFileTransferAgent.injectedFileTransferException;
      }
      return new InputStreamWithMetadata(countingStream.getCount(), null, tempStream);

    } catch (IOException ex) {
      logger.error("Exception compressing input stream", ex);

      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "error encountered for compression");
    }
  }

  private static InputStreamWithMetadata computeDigest(InputStream is, boolean resetStream)
      throws NoSuchAlgorithmException, IOException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    if (resetStream) {
      FileBackedOutputStream tempStream = new FileBackedOutputStream(MAX_BUFFER_SIZE, true);

      CountingOutputStream countingOutputStream = new CountingOutputStream(tempStream);

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);

      IOUtils.copy(is, digestStream);

      return new InputStreamWithMetadata(
          countingOutputStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          tempStream);
    } else {
      CountingOutputStream countingOutputStream =
          new CountingOutputStream(ByteStreams.nullOutputStream());

      DigestOutputStream digestStream = new DigestOutputStream(countingOutputStream, md);
      IOUtils.copy(is, digestStream);
      return new InputStreamWithMetadata(
          countingOutputStream.getCount(),
          Base64.getEncoder().encodeToString(digestStream.getMessageDigest().digest()),
          null);
    }
  }

  /**
   * A callable that can be executed in a separate thread using executor service.
   *
   * <p>The callable does compression if needed and upload the result to the table's staging area.
   *
   * @deprecated use {@link #getUploadFileCallable(StageInfo, String, FileMetadata,
   *     SnowflakeStorageClient, SFSession, String, InputStream, boolean, int, File,
   *     RemoteStoreFileEncryptionMaterial, String)}
   * @param stage information about the stage
   * @param srcFilePath source file path
   * @param metadata file metadata
   * @param client client object used to communicate with c3
   * @param session session object
   * @param command command string
   * @param inputStream null if upload source is file
   * @param sourceFromStream whether upload source is file or stream
   * @param parallel number of threads for parallel uploading
   * @param srcFile source file name
   * @param encMat not null if encryption is required
   * @return a callable that uploading file to the remote store
   */
  @Deprecated
  public static Callable<Void> getUploadFileCallable(
      final StageInfo stage,
      final String srcFilePath,
      final FileMetadata metadata,
      final SnowflakeStorageClient client,
      final SFSession session,
      final String command,
      final InputStream inputStream,
      final boolean sourceFromStream,
      final int parallel,
      final File srcFile,
      final RemoteStoreFileEncryptionMaterial encMat) {
    return getUploadFileCallable(
        stage,
        srcFilePath,
        metadata,
        client,
        session,
        command,
        inputStream,
        sourceFromStream,
        parallel,
        srcFile,
        encMat,
        null);
  }

  /**
   * A callable that can be executed in a separate thread using executor service.
   *
   * <p>The callable does compression if needed and upload the result to the table's staging area.
   *
   * @param stage information about the stage
   * @param srcFilePath source file path
   * @param metadata file metadata
   * @param client client object used to communicate with c3
   * @param session session object
   * @param command command string
   * @param inputStream null if upload source is file
   * @param sourceFromStream whether upload source is file or stream
   * @param parallel number of threads for parallel uploading
   * @param srcFile source file name
   * @param encMat not null if encryption is required
   * @param queryId last executed query id (for forwarding in possible exceptions)
   * @return a callable that uploading file to the remote store
   */
  public static Callable<Void> getUploadFileCallable(
      final StageInfo stage,
      final String srcFilePath,
      final FileMetadata metadata,
      final SnowflakeStorageClient client,
      final SFSession session,
      final String command,
      final InputStream inputStream,
      final boolean sourceFromStream,
      final int parallel,
      final File srcFile,
      final RemoteStoreFileEncryptionMaterial encMat,
      final String queryId) {
    return new Callable<Void>() {
      public Void call() throws Exception {

        logger.trace("Entering getUploadFileCallable...", false);

        // make sure initialize context for the telemetry service for this thread
        TelemetryService.getInstance().updateContext(session.getSnowflakeConnectionString());

        InputStream uploadStream = inputStream;

        File fileToUpload = null;

        if (uploadStream == null) {
          try {

            // Normal flow will never hit here. This is only for testing purposes
            if (isInjectedFileTransferExceptionEnabled()
                && SnowflakeFileTransferAgent.injectedFileTransferException
                    instanceof FileNotFoundException) {
              throw (FileNotFoundException)
                  SnowflakeFileTransferAgent.injectedFileTransferException;
            }

            FileUtil.logFileUsage(srcFilePath, "Get file to upload", false);
            uploadStream = new FileInputStream(srcFilePath);
          } catch (FileNotFoundException ex) {
            metadata.resultStatus = ResultStatus.ERROR;
            metadata.errorDetails = ex.getMessage();
            throw ex;
          }
        }

        // this shouldn't happen
        if (metadata == null) {
          throw new SnowflakeSQLLoggedException(
              queryId,
              session,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              "missing file metadata for: " + srcFilePath);
        }

        String destFileName = metadata.destFileName;

        long uploadSize;

        String digest = null;

        logger.debug("Dest file name: {}", false);

        // Temp file that needs to be cleaned up when upload was successful
        FileBackedOutputStream fileBackedOutputStream = null;

        // SNOW-16082: we should capture exception if we fail to compress or
        // calculate digest.
        try {
          if (metadata.requireCompress) {
            InputStreamWithMetadata compressedSizeAndStream =
                (encMat == null
                    ? compressStreamWithGZIPNoDigest(uploadStream, session, queryId)
                    : compressStreamWithGZIP(uploadStream, session, queryId));

            fileBackedOutputStream = compressedSizeAndStream.fileBackedOutputStream;

            // update the size
            uploadSize = compressedSizeAndStream.size;
            digest = compressedSizeAndStream.digest;

            if (compressedSizeAndStream.fileBackedOutputStream.getFile() != null) {
              fileToUpload = compressedSizeAndStream.fileBackedOutputStream.getFile();
            }

            logger.debug("New size after compression: {}", uploadSize);
          } else if (stage.getStageType() != StageInfo.StageType.LOCAL_FS) {
            // If it's not local_fs, we store our digest in the metadata
            // In local_fs, we don't need digest, and if we turn it on, we will consume whole
            // uploadStream, which local_fs uses.
            InputStreamWithMetadata result = computeDigest(uploadStream, sourceFromStream);
            digest = result.digest;
            fileBackedOutputStream = result.fileBackedOutputStream;
            uploadSize = result.size;

            if (!sourceFromStream) {
              fileToUpload = srcFile;
            } else if (result.fileBackedOutputStream.getFile() != null) {
              fileToUpload = result.fileBackedOutputStream.getFile();
            }
          } else {
            if (!sourceFromStream && (srcFile != null)) {
              fileToUpload = srcFile;
            }

            // if stage is local_fs and upload source is stream, upload size
            // does not matter since 1) transfer did not require size 2) no
            // output from uploadStream api is required
            uploadSize = sourceFromStream ? 0 : srcFile.length();
          }

          logger.debug(
              "Started copying file from: {} to {}:{} destName: {} "
                  + "auto compressed? {} size: {}",
              srcFilePath,
              stage.getStageType().name(),
              stage.getLocation(),
              destFileName,
              (metadata.requireCompress ? "yes" : "no"),
              uploadSize);

          // Simulated failure code.
          if (session.getInjectFileUploadFailure() != null
              && srcFilePath.endsWith((session).getInjectFileUploadFailure())) {
            throw new SnowflakeSimulatedUploadFailure(
                srcFile != null ? srcFile.getName() : "Unknown");
          }

          // upload it
          switch (stage.getStageType()) {
            case LOCAL_FS:
              pushFileToLocal(
                  stage.getLocation(),
                  srcFilePath,
                  destFileName,
                  uploadStream,
                  fileBackedOutputStream,
                  session,
                  queryId);
              break;

            case S3:
            case AZURE:
            case GCS:
              pushFileToRemoteStore(
                  stage,
                  destFileName,
                  uploadStream,
                  fileBackedOutputStream,
                  uploadSize,
                  digest,
                  metadata.destCompressionType,
                  client,
                  session,
                  command,
                  parallel,
                  fileToUpload,
                  (fileToUpload == null),
                  encMat,
                  null,
                  null,
                  queryId);
              metadata.isEncrypted = encMat != null;
              break;
          }
        } catch (SnowflakeSimulatedUploadFailure ex) {
          // This code path is used for Simulated failure code in tests.
          // Never happen in production
          metadata.resultStatus = ResultStatus.ERROR;
          metadata.errorDetails = ex.getMessage();
          throw ex;
        } catch (Throwable ex) {
          logger.error("Exception encountered during file upload", ex);
          metadata.resultStatus = ResultStatus.ERROR;
          metadata.errorDetails = ex.getMessage();
          throw ex;
        } finally {
          if (fileBackedOutputStream != null) {
            try {
              fileBackedOutputStream.reset();
            } catch (IOException ex) {
              logger.debug("Failed to clean up temp file: {}", ex);
            }
          }
          if (inputStream == null) {
            IOUtils.closeQuietly(uploadStream);
          }
        }

        logger.debug("FilePath: {}", srcFilePath);

        // set dest size
        metadata.destFileSize = uploadSize;

        // mark the file as being uploaded
        metadata.resultStatus = ResultStatus.UPLOADED;

        return null;
      }
    };
  }

  /**
   * A callable that can be executed in a separate thread using executor service.
   *
   * <p>The callable download files from a stage location to a local location
   *
   * @deprecated use {@link #getDownloadFileCallable(StageInfo, String, String, Map,
   *     SnowflakeStorageClient, SFSession, String, int, RemoteStoreFileEncryptionMaterial, String,
   *     String)}
   * @param stage stage information
   * @param srcFilePath path that stores the downloaded file
   * @param localLocation local location
   * @param fileMetadataMap file metadata map
   * @param client remote store client
   * @param session session object
   * @param command command string
   * @param encMat remote store encryption material
   * @param parallel number of parallel threads for downloading
   * @param presignedUrl Presigned URL for file download
   * @return a callable responsible for downloading files
   */
  @Deprecated
  public static Callable<Void> getDownloadFileCallable(
      final StageInfo stage,
      final String srcFilePath,
      final String localLocation,
      final Map<String, FileMetadata> fileMetadataMap,
      final SnowflakeStorageClient client,
      final SFSession session,
      final String command,
      final int parallel,
      final RemoteStoreFileEncryptionMaterial encMat,
      final String presignedUrl) {
    return getDownloadFileCallable(
        stage,
        srcFilePath,
        localLocation,
        fileMetadataMap,
        client,
        session,
        command,
        parallel,
        encMat,
        presignedUrl,
        null);
  }

  /**
   * A callable that can be executed in a separate thread using executor service.
   *
   * <p>The callable download files from a stage location to a local location
   *
   * @param stage stage information
   * @param srcFilePath path that stores the downloaded file
   * @param localLocation local location
   * @param fileMetadataMap file metadata map
   * @param client remote store client
   * @param session session object
   * @param command command string
   * @param encMat remote store encryption material
   * @param parallel number of parallel threads for downloading
   * @param presignedUrl Presigned URL for file download
   * @param queryId the query ID
   * @return a callable responsible for downloading files
   */
  public static Callable<Void> getDownloadFileCallable(
      final StageInfo stage,
      final String srcFilePath,
      final String localLocation,
      final Map<String, FileMetadata> fileMetadataMap,
      final SnowflakeStorageClient client,
      final SFSession session,
      final String command,
      final int parallel,
      final RemoteStoreFileEncryptionMaterial encMat,
      final String presignedUrl,
      final String queryId) {
    return new Callable<Void>() {
      public Void call() throws Exception {

        logger.debug("Entering getDownloadFileCallable...", false);

        // make sure initialize context for the telemetry service for this thread
        TelemetryService.getInstance().updateContext(session.getSnowflakeConnectionString());

        FileMetadata metadata = fileMetadataMap.get(srcFilePath);

        // this shouldn't happen
        if (metadata == null) {
          throw new SnowflakeSQLLoggedException(
              queryId,
              session,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              "missing file metadata for: " + srcFilePath);
        }

        String destFileName = metadata.destFileName;
        logger.debug(
            "Started copying file from: {}:{} file path:{} to {} destName:{}",
            stage.getStageType().name(),
            stage.getLocation(),
            srcFilePath,
            localLocation,
            destFileName);

        try {
          switch (stage.getStageType()) {
            case LOCAL_FS:
              pullFileFromLocal(
                  stage.getLocation(), srcFilePath, localLocation, destFileName, session, queryId);
              break;

            case AZURE:
            case S3:
            case GCS:
              pullFileFromRemoteStore(
                  stage,
                  srcFilePath,
                  destFileName,
                  localLocation,
                  client,
                  session,
                  command,
                  parallel,
                  encMat,
                  presignedUrl,
                  queryId);
              metadata.isEncrypted = encMat != null;
              break;
          }
        } catch (Throwable ex) {
          logger.error("Exception encountered during file download", ex);

          metadata.resultStatus = ResultStatus.ERROR;
          metadata.errorDetails = ex.getMessage();
          throw ex;
        }

        logger.debug("FilePath: {}", srcFilePath);

        File destFile = new File(localLocation + localFSFileSep + destFileName);
        long downloadSize = destFile.length();

        // set dest size
        metadata.destFileSize = downloadSize;

        // mark the file as being uploaded
        metadata.resultStatus = ResultStatus.DOWNLOADED;

        return null;
      }
    };
  }

  public SnowflakeFileTransferAgent(String command, SFSession session, SFStatement statement)
      throws SnowflakeSQLException {
    this.command = command;
    this.session = session;
    this.statement = statement;

    // parse the command
    logger.debug("Start parsing", false);

    parseCommand();

    if (stageInfo.getStageType() != StageInfo.StageType.LOCAL_FS) {
      storageClient = storageFactory.createClient(stageInfo, parallel, null, session);
    }
  }

  /**
   * Parse the put/get command.
   *
   * <p>We send the command to the GS to do the parsing. In the future, we will delegate more work
   * to GS such as copying files from HTTP to the remote store.
   *
   * @throws SnowflakeSQLException failure to parse the PUT/GET command
   */
  private void parseCommand() throws SnowflakeSQLException {
    // For AWS and Azure, this command returns enough info for us to get creds
    // we can use for each of the GETs/PUTs. For GCS, we need to issue a separate
    // call to GS to get creds (in the form of a presigned URL) for each file
    // we're uploading or downloading. This call gets our src_location and
    // encryption material, which we'll use for all the subsequent calls to GS
    // for creds for each file. Those calls are made from pushFileToRemoteStore
    // and pullFileFromRemoteStore if the storage client requires a presigned
    // URL.
    JsonNode jsonNode = parseCommandInGS(statement, command);

    // get command type
    if (!jsonNode.path("data").path("command").isMissingNode()) {
      commandType = CommandType.valueOf(jsonNode.path("data").path("command").asText());
    }

    // get source file locations as array (apply to both upload and download)
    JsonNode locationsNode = jsonNode.path("data").path("src_locations");
    if (!locationsNode.isArray()) {
      throw new SnowflakeSQLException(
          queryID, ErrorCode.INTERNAL_ERROR, "src_locations must be an array");
    }

    queryID = jsonNode.path("data").path("queryId").asText();

    String[] src_locations;

    try {
      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()
          && injectedFileTransferException instanceof SnowflakeSQLException) {
        throw (SnowflakeSQLException) SnowflakeFileTransferAgent.injectedFileTransferException;
      }

      src_locations = mapper.readValue(locationsNode.toString(), String[].class);
      initEncryptionMaterial(commandType, jsonNode);
      initPresignedUrls(commandType, jsonNode);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryID,
          ex,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Failed to parse the locations due to: " + ex.getMessage());
    }

    showEncryptionParameter =
        jsonNode.path("data").path("clientShowEncryptionParameter").asBoolean();

    JsonNode thresholdNode = jsonNode.path("data").path("threshold");
    int threshold = thresholdNode.asInt();
    // if value is <= 0, this means an error was made in parsing the threshold or the threshold is
    // invalid.
    // Only use the threshold value if it is valid.
    if (threshold > 0) {
      bigFileThreshold = threshold;
    }

    String localFilePathFromGS = null;

    // do upload command specific parsing
    if (commandType == CommandType.UPLOAD) {
      if (src_locations.length > 0) {
        localFilePathFromGS = src_locations[0];
      }

      sourceFiles = expandFileNames(src_locations, queryID);

      autoCompress = jsonNode.path("data").path("autoCompress").asBoolean(true);

      if (!jsonNode.path("data").path("sourceCompression").isMissingNode()) {
        sourceCompression = jsonNode.path("data").path("sourceCompression").asText();
      }

    } else {
      // do download command specific parsing
      srcFileToEncMat = new HashMap<>();

      // create mapping from source file to encryption materials
      if (src_locations.length == encryptionMaterial.size()) {
        for (int srcFileIdx = 0; srcFileIdx < src_locations.length; srcFileIdx++) {
          srcFileToEncMat.put(src_locations[srcFileIdx], encryptionMaterial.get(srcFileIdx));
        }
      }

      // create mapping from source file to presigned URLs
      srcFileToPresignedUrl = new HashMap<>();
      if (src_locations.length == presignedUrls.size()) {
        for (int srcFileIdx = 0; srcFileIdx < src_locations.length; srcFileIdx++) {
          srcFileToPresignedUrl.put(src_locations[srcFileIdx], presignedUrls.get(srcFileIdx));
        }
      }

      sourceFiles = new HashSet<String>(Arrays.asList(src_locations));

      localLocation = jsonNode.path("data").path("localLocation").asText();

      localFilePathFromGS = localLocation;

      if (localLocation.startsWith("~")) {
        // replace ~ with user home
        localLocation = systemGetProperty("user.home") + localLocation.substring(1);
      }

      // it should not start with any ~ after the above replacement
      if (localLocation.startsWith("~")) {
        throw new SnowflakeSQLLoggedException(
            queryID,
            session,
            ErrorCode.PATH_NOT_DIRECTORY.getMessageCode(),
            SqlState.IO_ERROR,
            localLocation);
      }

      // todo: replace ~userid with the home directory of a given userid
      // one idea is to get the home directory for current user and replace
      // the last user id with the given user id.

      // user may also specify files relative to current directory
      // add the current path if that is the case
      if (!(new File(localLocation)).isAbsolute()) {
        String cwd = systemGetProperty("user.dir");

        logger.debug("Adding current working dir to relative file path.", false);

        localLocation = cwd + localFSFileSep + localLocation;
      }

      // local location should be a directory
      if ((new File(localLocation)).isFile()) {
        throw new SnowflakeSQLLoggedException(
            queryID,
            session,
            ErrorCode.PATH_NOT_DIRECTORY.getMessageCode(),
            SqlState.IO_ERROR,
            localLocation);
      }
    }

    // SNOW-15153: verify that the value after file:// is not changed by GS
    verifyLocalFilePath(localFilePathFromGS);

    parallel = jsonNode.path("data").path("parallel").asInt();

    overwrite = jsonNode.path("data").path("overwrite").asBoolean(false);

    stageInfo = getStageInfo(jsonNode, this.session);

    if (logger.isDebugEnabled()) {
      logger.debug("Command type: {}", commandType);

      if (commandType == CommandType.UPLOAD) {
        logger.debug("Auto compress: {}, source compression: {}", autoCompress, sourceCompression);
      } else {
        logger.debug("Local download location: {}", localLocation);
      }

      logger.debug("Source files: {}", String.join(",", sourceFiles));
      logger.debug(
          "stageLocation: {}, parallel: {}, overwrite: {}, destLocationType: {}, stageRegion: {},"
              + " endPoint: {}, storageAccount: {}",
          stageInfo.getLocation(),
          parallel,
          overwrite,
          stageInfo.getStageType(),
          stageInfo.getRegion(),
          stageInfo.getEndPoint(),
          stageInfo.getStorageAccount());
    }
  }

  /**
   * Construct Stage Info object from JsonNode.
   *
   * @param jsonNode JsonNode to use serialize into StageInfo Object
   * @param session can be null.
   * @return StageInfo constructed from JsonNode and session params.
   * @throws SnowflakeSQLException
   */
  static StageInfo getStageInfo(JsonNode jsonNode, SFSession session) throws SnowflakeSQLException {
    String queryId = jsonNode.path("data").path("queryId").asText();

    // more parameters common to upload/download
    String stageLocation = jsonNode.path("data").path("stageInfo").path("location").asText();

    String stageLocationType =
        jsonNode.path("data").path("stageInfo").path("locationType").asText();

    String stageRegion = null;
    if (!jsonNode.path("data").path("stageInfo").path("region").isMissingNode()) {
      stageRegion = jsonNode.path("data").path("stageInfo").path("region").asText();
    }

    boolean isClientSideEncrypted = true;
    if (!jsonNode.path("data").path("stageInfo").path("isClientSideEncrypted").isMissingNode()) {
      isClientSideEncrypted =
          jsonNode.path("data").path("stageInfo").path("isClientSideEncrypted").asBoolean(true);
    }

    // endPoint is currently known to be set for Azure stages or S3. For S3 it will be set
    // specifically
    // for FIPS or VPCE S3 endpoint. SNOW-652696
    String endPoint = null;
    if ("AZURE".equalsIgnoreCase(stageLocationType)
        || "S3".equalsIgnoreCase(stageLocationType)
        || "GCS".equalsIgnoreCase(stageLocationType)) {
      endPoint = jsonNode.path("data").path("stageInfo").findValue("endPoint").asText();
      if ("GCS".equalsIgnoreCase(stageLocationType)
          && endPoint != null
          && (endPoint.trim().isEmpty() || "null".equals(endPoint))) {
        // setting to null to preserve previous behaviour for GCS
        endPoint = null;
      }
    }

    String stgAcct = null;
    // storageAccount are only available in Azure stages. Value
    // will be present but null in other platforms.
    if ("AZURE".equalsIgnoreCase(stageLocationType)) {
      // Jackson is doing some very strange things trying to pull the value of
      // the storageAccount node after adding the GCP library dependencies.
      // If we try to pull the value by name, we get back null, but clearly the
      // node is there. This code works around the issue by enumerating through
      // all the nodes and getting the one that starts with "sto". The value
      // then comes back with double quotes around it, so we're stripping them
      // off. As long as our JSON doc doesn't add another node that starts with
      // "sto", this should work fine.
      Iterator<Entry<String, JsonNode>> fields = jsonNode.path("data").path("stageInfo").fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> jsonField = fields.next();
        if (jsonField.getKey().startsWith("sto")) {
          stgAcct =
              jsonField
                  .getValue()
                  .toString()
                  .trim()
                  .substring(1, jsonField.getValue().toString().trim().lastIndexOf("\""));
        }
      }
    }

    if ("LOCAL_FS".equalsIgnoreCase(stageLocationType)) {
      if (stageLocation.startsWith("~")) {
        // replace ~ with user home
        stageLocation = systemGetProperty("user.home") + stageLocation.substring(1);
      }

      if (!(new File(stageLocation)).isAbsolute()) {
        String cwd = systemGetProperty("user.dir");

        logger.debug("Adding current working dir to stage file path.");

        stageLocation = cwd + localFSFileSep + stageLocation;
      }
    }

    Map<?, ?> stageCredentials = extractStageCreds(jsonNode, queryId);

    StageInfo stageInfo =
        StageInfo.createStageInfo(
            stageLocationType,
            stageLocation,
            stageCredentials,
            stageRegion,
            endPoint,
            stgAcct,
            isClientSideEncrypted);

    // Setup pre-signed URL into stage info if pre-signed URL is returned.
    if (stageInfo.getStageType() == StageInfo.StageType.GCS) {
      JsonNode presignedUrlNode = jsonNode.path("data").path("stageInfo").path("presignedUrl");
      if (!presignedUrlNode.isMissingNode()) {
        String presignedUrl = presignedUrlNode.asText();
        if (!Strings.isNullOrEmpty(presignedUrl)) {
          stageInfo.setPresignedUrl(presignedUrl);
        }
      }
    }

    setupUseRegionalUrl(jsonNode, stageInfo);

    if (stageInfo.getStageType() == StageInfo.StageType.S3) {
      if (session == null) {
        // This node's value is set if PUT is used without Session. (For Snowpipe Streaming, we rely
        // on a response from a server to have this field set to use S3RegionalURL)
        JsonNode useS3RegionalURLNode =
            jsonNode.path("data").path("stageInfo").path("useS3RegionalUrl");
        if (!useS3RegionalURLNode.isMissingNode()) {
          boolean useS3RegionalUrl = useS3RegionalURLNode.asBoolean(false);
          stageInfo.setUseS3RegionalUrl(useS3RegionalUrl);
        }
      } else {
        // Update StageInfo to reflect use of S3 regional URL.
        // This is required for connecting to S3 over privatelink when the
        // target stage is in us-east-1
        stageInfo.setUseS3RegionalUrl(session.getUseRegionalS3EndpointsForPresignedURL());
      }
    }

    return stageInfo;
  }

  private static void setupUseRegionalUrl(JsonNode jsonNode, StageInfo stageInfo) {
    if (stageInfo.getStageType() != StageInfo.StageType.GCS
        && stageInfo.getStageType() != StageInfo.StageType.S3) {
      return;
    }
    JsonNode useRegionalURLNode = jsonNode.path("data").path("stageInfo").path("useRegionalUrl");
    if (!useRegionalURLNode.isMissingNode()) {
      boolean useRegionalURL = useRegionalURLNode.asBoolean(false);
      stageInfo.setUseRegionalUrl(useRegionalURL);
    }
  }

  /**
   * A helper method to verify if the local file path from GS matches what's parsed locally. This is
   * for security purpose as documented in SNOW-15153.
   *
   * @param localFilePathFromGS the local file path to verify
   * @throws SnowflakeSQLException Will be thrown if the log path if empty or if it doesn't match
   *     what comes back from GS
   */
  private void verifyLocalFilePath(String localFilePathFromGS) throws SnowflakeSQLException {
    String localFilePath = getLocalFilePathFromCommand(command, true);

    if (!localFilePath.isEmpty() && !localFilePath.equals(localFilePathFromGS)) {
      throw new SnowflakeSQLLoggedException(
          queryID,
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Unexpected local file path from GS. From GS: "
              + localFilePathFromGS
              + ", expected: "
              + localFilePath);
    } else if (localFilePath.isEmpty()) {
      logger.debug("Fail to parse local file path from command: {}", command);
    } else {
      logger.trace("local file path from GS matches local parsing: {}", localFilePath);
    }
  }

  /**
   * Parses out the local file path from the command. We need this to get the file paths to expand
   * wildcards and make sure the paths GS returns are correct
   *
   * @param command The GET/PUT command we send to GS
   * @param unescape True to unescape backslashes coming from GS
   * @return Path to the local file
   */
  private static String getLocalFilePathFromCommand(String command, boolean unescape) {
    if (command == null) {
      logger.error("null command", false);
      return null;
    }

    if (command.indexOf(FILE_PROTOCOL) < 0) {
      logger.error("file:// prefix not found in command: {}", command);
      return null;
    }

    int localFilePathBeginIdx = command.indexOf(FILE_PROTOCOL) + FILE_PROTOCOL.length();
    boolean isLocalFilePathQuoted =
        (localFilePathBeginIdx > FILE_PROTOCOL.length())
            && (command.charAt(localFilePathBeginIdx - 1 - FILE_PROTOCOL.length()) == '\'');

    // the ending index is exclusive
    int localFilePathEndIdx = 0;
    String localFilePath = "";

    if (isLocalFilePathQuoted) {
      // look for the matching quote
      localFilePathEndIdx = command.indexOf("'", localFilePathBeginIdx);
      if (localFilePathEndIdx > localFilePathBeginIdx) {
        localFilePath = command.substring(localFilePathBeginIdx, localFilePathEndIdx);
      }
      // unescape backslashes to match the file name from GS
      if (unescape) {
        localFilePath = localFilePath.replaceAll("\\\\\\\\", "\\\\");
      }
    } else {
      // look for the first space or new line or semi colon
      List<Integer> indexList = new ArrayList<>();
      char[] delimiterChars = {' ', '\n', ';'};
      for (int i = 0; i < delimiterChars.length; i++) {
        int charIndex = command.indexOf(delimiterChars[i], localFilePathBeginIdx);
        if (charIndex != -1) {
          indexList.add(charIndex);
        }
      }

      localFilePathEndIdx = indexList.isEmpty() ? -1 : Collections.min(indexList);

      if (localFilePathEndIdx > localFilePathBeginIdx) {
        localFilePath = command.substring(localFilePathBeginIdx, localFilePathEndIdx);
      } else if (localFilePathEndIdx == -1) {
        localFilePath = command.substring(localFilePathBeginIdx);
      }
    }

    return localFilePath;
  }

  /**
   * @return JSON doc containing the command options returned by GS
   * @throws SnowflakeSQLException Will be thrown if parsing the command by GS fails
   */
  private static JsonNode parseCommandInGS(SFStatement statement, String command)
      throws SnowflakeSQLException {
    Object result = null;
    // send the command to GS
    try {
      result =
          statement.executeHelper(
              command,
              "application/json",
              null, // bindValues
              false, // describeOnly
              false, // internal
              false, // async
              new ExecTimeTelemetryData()); // OOB telemetry timing queries
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getQueryId(), ex, ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    JsonNode jsonNode = (JsonNode) result;
    logger.debug("Response: {}", jsonNode.toString());

    SnowflakeUtil.checkErrorAndThrowException(jsonNode);
    return jsonNode;
  }

  /**
   * @param rootNode JSON doc returned by GS
   * @throws SnowflakeSQLException Will be thrown if we fail to parse the stage credentials
   */
  private static Map<?, ?> extractStageCreds(JsonNode rootNode, String queryId)
      throws SnowflakeSQLException {
    JsonNode credsNode = rootNode.path("data").path("stageInfo").path("creds");
    Map<?, ?> stageCredentials = null;

    try {
      TypeReference<HashMap<String, String>> typeRef =
          new TypeReference<HashMap<String, String>>() {};
      stageCredentials = mapper.readValue(credsNode.toString(), typeRef);

    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
          ex,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Failed to parse the credentials ("
              + (credsNode != null ? credsNode.toString() : "null")
              + ") due to exception: "
              + ex.getMessage());
    }

    return stageCredentials;
  }

  /**
   * This is API function to retrieve the File Transfer Metadatas.
   *
   * <p>NOTE: It only supports PUT on S3/AZURE/GCS
   *
   * @return The file transfer metadatas for to-be-transferred files.
   * @throws SnowflakeSQLException if any error occurs
   */
  public List<SnowflakeFileTransferMetadata> getFileTransferMetadatas()
      throws SnowflakeSQLException {
    List<SnowflakeFileTransferMetadata> result = new ArrayList<>();
    if (stageInfo.getStageType() != StageInfo.StageType.GCS
        && stageInfo.getStageType() != StageInfo.StageType.AZURE
        && stageInfo.getStageType() != StageInfo.StageType.S3) {
      throw new SnowflakeSQLLoggedException(
          queryID,
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "This API only supports S3/AZURE/GCS");
    }

    if (commandType != CommandType.UPLOAD) {
      throw new SnowflakeSQLLoggedException(
          queryID,
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "This API only supports PUT command");
    }

    for (String sourceFilePath : sourceFiles) {
      String sourceFileName = sourceFilePath.substring(sourceFilePath.lastIndexOf("/") + 1);
      result.add(
          new SnowflakeFileTransferMetadataV1(
              stageInfo.getPresignedUrl(),
              sourceFileName,
              encryptionMaterial.get(0).getQueryStageMasterKey(),
              encryptionMaterial.get(0).getQueryId(),
              encryptionMaterial.get(0).getSmkId(),
              commandType,
              stageInfo));
    }

    return result;
  }

  /**
   * This is API function to parse the File Transfer Metadatas from a supplied PUT call response.
   *
   * <p>NOTE: It only supports PUT on S3/AZURE/GCS (i.e. NOT LOCAL_FS)
   *
   * <p>It also assumes there is no active SFSession
   *
   * @param jsonNode JSON doc returned by GS from PUT call
   * @return The file transfer metadatas for to-be-transferred files.
   * @throws SnowflakeSQLException if any error occurs
   */
  public static List<SnowflakeFileTransferMetadata> getFileTransferMetadatas(JsonNode jsonNode)
      throws SnowflakeSQLException {
    return getFileTransferMetadatas(jsonNode, null);
  }

  /**
   * This is API function to parse the File Transfer Metadatas from a supplied PUT call response.
   *
   * <p>NOTE: It only supports PUT on S3/AZURE/GCS (i.e. NOT LOCAL_FS)
   *
   * <p>It also assumes there is no active SFSession
   *
   * @param jsonNode JSON doc returned by GS from PUT call
   * @param queryId String last executed query id if available
   * @return The file transfer metadatas for to-be-transferred files.
   * @throws SnowflakeSQLException if any error occurs
   */
  public static List<SnowflakeFileTransferMetadata> getFileTransferMetadatas(
      JsonNode jsonNode, String queryId) throws SnowflakeSQLException {
    CommandType commandType =
        !jsonNode.path("data").path("command").isMissingNode()
            ? CommandType.valueOf(jsonNode.path("data").path("command").asText())
            : CommandType.UPLOAD;
    if (commandType != CommandType.UPLOAD) {
      throw new SnowflakeSQLException(
          queryId, ErrorCode.INTERNAL_ERROR, "This API only supports PUT commands");
    }

    JsonNode locationsNode = jsonNode.path("data").path("src_locations");

    if (!locationsNode.isArray()) {
      throw new SnowflakeSQLException(
          queryId, ErrorCode.INTERNAL_ERROR, "src_locations must be an array");
    }

    final String[] srcLocations;
    final List<RemoteStoreFileEncryptionMaterial> encryptionMaterial;
    try {
      srcLocations = mapper.readValue(locationsNode.toString(), String[].class);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Failed to parse the locations due to: " + ex.getMessage());
    }

    try {
      encryptionMaterial = getEncryptionMaterial(commandType, jsonNode);
    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Failed to parse encryptionMaterial due to: " + ex.getMessage());
    }

    // For UPLOAD we expect encryptionMaterial to have length 1
    if (encryptionMaterial.size() != 1) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Encryption material for UPLOAD should have size 1 but have "
              + encryptionMaterial.size());
    }

    final Set<String> sourceFiles = expandFileNames(srcLocations, queryId);

    StageInfo stageInfo = getStageInfo(jsonNode, null /*SFSession*/);

    List<SnowflakeFileTransferMetadata> result = new ArrayList<>();
    if (stageInfo.getStageType() != StageInfo.StageType.GCS
        && stageInfo.getStageType() != StageInfo.StageType.AZURE
        && stageInfo.getStageType() != StageInfo.StageType.S3) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "This API only supports S3/AZURE/GCS, received=" + stageInfo.getStageType());
    }

    for (String sourceFilePath : sourceFiles) {
      String sourceFileName = sourceFilePath.substring(sourceFilePath.lastIndexOf("/") + 1);
      result.add(
          new SnowflakeFileTransferMetadataV1(
              stageInfo.getPresignedUrl(),
              sourceFileName,
              encryptionMaterial.get(0) != null
                  ? encryptionMaterial.get(0).getQueryStageMasterKey()
                  : null,
              encryptionMaterial.get(0) != null ? encryptionMaterial.get(0).getQueryId() : null,
              encryptionMaterial.get(0) != null ? encryptionMaterial.get(0).getSmkId() : null,
              commandType,
              stageInfo));
    }

    return result;
  }

  @Override
  public boolean execute() throws SQLException {
    try {
      logger.debug("Start init metadata");

      // initialize file metadata map
      initFileMetadata();

      logger.debug("Start checking file types");

      // check file compression type
      if (commandType == CommandType.UPLOAD) {
        processFileCompressionTypes();
      }

      // Filter out files that are already existing in the destination.
      // GCS may or may not use presigned URL
      if (!overwrite
          && (stageInfo.getStageType() != StageInfo.StageType.GCS
              || !storageClient.requirePresignedUrl())) {
        logger.debug("Start filtering");

        filterExistingFiles();

        logger.debug("Filtering done");
      }

      synchronized (canceled) {
        if (canceled) {
          logger.debug("File transfer canceled by user");
          threadExecutor = null;
          return false;
        }
      }

      // create target directory for download command
      if (commandType == CommandType.DOWNLOAD) {
        File dir = new File(localLocation);
        if (!dir.exists()) {
          boolean created = dir.mkdirs();

          if (created) {
            logger.debug("Directory created: {}", localLocation);
          } else {
            logger.debug("Directory not created {}", localLocation);
          }
        }

        downloadFiles();
      } else if (sourceFromStream) {
        uploadStream();
      } else {
        // separate files to big files list and small files list
        // big files will be uploaded in serial, while small files will be
        // uploaded concurrently.
        logger.debug("Start segregate files by size");
        segregateFilesBySize();

        if (bigSourceFiles != null) {
          logger.debug("Start uploading big files");
          uploadFiles(bigSourceFiles, 1);
          logger.debug("End uploading big files");
        }

        if (smallSourceFiles != null) {
          logger.debug("Start uploading small files");
          uploadFiles(smallSourceFiles, parallel);
          logger.debug("End uploading small files");
        }
      }

      // populate status rows to be returned to the client
      populateStatusRows();

      return true;
    } finally {
      if (storageClient != null) {
        storageClient.shutdown();
      }
    }
  }

  /** Helper to upload data from a stream */
  private void uploadStream() throws SnowflakeSQLException {
    try {
      FileMetadata fileMetadata = fileMetadataMap.get(SRC_FILE_NAME_FOR_STREAM);

      if (fileMetadata.resultStatus == ResultStatus.SKIPPED) {
        logger.debug(
            "Skipping {}, status: {}, details: {}",
            SRC_FILE_NAME_FOR_STREAM,
            fileMetadata.resultStatus,
            fileMetadata.errorDetails);
        return;
      }
      threadExecutor = SnowflakeUtil.createDefaultExecutorService("sf-stream-upload-worker-", 1);

      RemoteStoreFileEncryptionMaterial encMat = encryptionMaterial.get(0);
      Future<Void> uploadTask = null;
      if (commandType == CommandType.UPLOAD) {
        uploadTask =
            threadExecutor.submit(
                getUploadFileCallable(
                    stageInfo,
                    SRC_FILE_NAME_FOR_STREAM,
                    fileMetadata,
                    (stageInfo.getStageType() == StageInfo.StageType.LOCAL_FS)
                        ? null
                        : storageFactory.createClient(stageInfo, parallel, encMat, session),
                    session,
                    command,
                    sourceStream,
                    true,
                    parallel,
                    null,
                    encMat,
                    queryID));
      } else if (commandType == CommandType.DOWNLOAD) {
        throw new SnowflakeSQLLoggedException(
            queryID, session, ErrorCode.INTERNAL_ERROR.getMessageCode(), SqlState.INTERNAL_ERROR);
      }

      threadExecutor.shutdown();

      try {
        // Normal flow will never hit here. This is only for testing purposes
        if (isInjectedFileTransferExceptionEnabled()
            && SnowflakeFileTransferAgent.injectedFileTransferException
                instanceof InterruptedException) {
          throw (InterruptedException) SnowflakeFileTransferAgent.injectedFileTransferException;
        }

        // wait for the task to complete
        uploadTask.get();
      } catch (InterruptedException ex) {
        throw new SnowflakeSQLLoggedException(
            queryID, session, ErrorCode.INTERRUPTED.getMessageCode(), SqlState.QUERY_CANCELED);
      } catch (ExecutionException ex) {
        throw new SnowflakeSQLException(
            queryID,
            ex.getCause(),
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode());
      }
      logger.debug("Done with uploading from a stream");
    } finally {
      if (threadExecutor != null) {
        threadExecutor.shutdownNow();
        threadExecutor = null;
      }
    }
  }

  /** Download a file from remote, and return an input stream */
  @Override
  public InputStream downloadStream(String fileName) throws SnowflakeSQLException {
    logger.debug("Downloading file as stream: {}", fileName);
    if (stageInfo.getStageType() == StageInfo.StageType.LOCAL_FS) {
      logger.error("downloadStream function doesn't support local file system", false);

      throw new SnowflakeSQLException(
          queryID,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          session,
          "downloadStream function only supported in remote stages");
    }

    remoteLocation remoteLocation = extractLocationAndPath(stageInfo.getLocation());

    // when downloading files as stream there should be only one file in source files
    String sourceLocation =
        sourceFiles.stream()
            .findFirst()
            .orElseThrow(
                () ->
                    new SnowflakeSQLException(
                        queryID,
                        SqlState.NO_DATA,
                        ErrorCode.FILE_NOT_FOUND.getMessageCode(),
                        session,
                        "File not found: " + fileName));

    if (!fileName.equals(sourceLocation)) {
      // filename may be different from source location e.g. in git repositories
      logger.debug("Changing file to download location from {} to {}", fileName, sourceLocation);
    }
    String stageFilePath = sourceLocation;

    if (!remoteLocation.path.isEmpty()) {
      stageFilePath = SnowflakeUtil.concatFilePathNames(remoteLocation.path, sourceLocation, "/");
    }
    logger.debug("Stage file path for {} is {}", sourceLocation, stageFilePath);

    RemoteStoreFileEncryptionMaterial encMat = srcFileToEncMat.get(sourceLocation);
    String presignedUrl = srcFileToPresignedUrl.get(sourceLocation);

    return storageFactory
        .createClient(stageInfo, parallel, encMat, session)
        .downloadToStream(
            session,
            command,
            parallel,
            remoteLocation.location,
            stageFilePath,
            stageInfo.getRegion(),
            presignedUrl,
            queryID);
  }

  /** Helper to download files from remote */
  private void downloadFiles() throws SnowflakeSQLException {
    try {
      threadExecutor = SnowflakeUtil.createDefaultExecutorService("sf-file-download-worker-", 1);

      for (String srcFile : sourceFiles) {
        FileMetadata fileMetadata = fileMetadataMap.get(srcFile);

        // Check if the result status is already set so that we don't need to
        // upload it
        if (fileMetadata.resultStatus != ResultStatus.UNKNOWN) {
          logger.debug(
              "Skipping {}, status: {}, details: {}",
              srcFile,
              fileMetadata.resultStatus,
              fileMetadata.errorDetails);
          continue;
        }

        RemoteStoreFileEncryptionMaterial encMat = srcFileToEncMat.get(srcFile);
        String presignedUrl = srcFileToPresignedUrl.get(srcFile);
        threadExecutor.submit(
            getDownloadFileCallable(
                stageInfo,
                srcFile,
                localLocation,
                fileMetadataMap,
                (stageInfo.getStageType() == StageInfo.StageType.LOCAL_FS)
                    ? null
                    : storageFactory.createClient(stageInfo, parallel, encMat, session),
                session,
                command,
                parallel,
                encMat,
                presignedUrl,
                queryID));

        logger.debug("Submitted download job for: {}", srcFile);
      }

      threadExecutor.shutdown();

      try {
        // wait for all threads to complete without timeout
        threadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException ex) {
        throw new SnowflakeSQLLoggedException(
            queryID, session, ErrorCode.INTERRUPTED.getMessageCode(), SqlState.QUERY_CANCELED);
      }
      logger.debug("Done with downloading");
    } finally {
      if (threadExecutor != null) {
        threadExecutor.shutdownNow();
        threadExecutor = null;
      }
    }
  }

  /**
   * This method is used in uploadFiles to delay the file upload for the given time, which is set as
   * a session parameter called "inject_wait_in_put." Normally this value is 0, but it is used in
   * testing.
   *
   * @param delayTime the number of seconds to sleep before uploading the file
   */
  private void setUploadDelay(int delayTime) {
    if (delayTime > 0) {
      try {
        TimeUnit.SECONDS.sleep(delayTime);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * This method create a thread pool based on requested number of threads and upload the files
   * using the thread pool.
   *
   * @param fileList The set of files to upload
   * @param parallel degree of parallelism for the upload
   * @throws SnowflakeSQLException Will be thrown if uploading the files fails
   */
  private void uploadFiles(Set<String> fileList, int parallel) throws SnowflakeSQLException {
    try {
      threadExecutor =
          SnowflakeUtil.createDefaultExecutorService("sf-file-upload-worker-", parallel);

      for (String srcFile : fileList) {
        FileMetadata fileMetadata = fileMetadataMap.get(srcFile);

        // Check if the result status is already set so that we don't need to
        // upload it
        if (fileMetadata.resultStatus != ResultStatus.UNKNOWN) {
          logger.debug(
              "Skipping {}, status: {}, details: {}",
              srcFile,
              fileMetadata.resultStatus,
              fileMetadata.errorDetails);

          continue;
        }

        /*
         * For small files, we upload files in parallel, so we don't
         * want the remote store uploader to upload parts in parallel for each file.
         * For large files, we upload them in serial, and we want remote store uploader
         * to upload parts in parallel for each file. This is the reason
         * for the parallel value.
         */
        File srcFileObj = new File(srcFile);
        // PUT delay goes here!!
        int delay = session.getInjectWaitInPut();
        setUploadDelay(delay);

        threadExecutor.submit(
            getUploadFileCallable(
                stageInfo,
                srcFile,
                fileMetadata,
                (stageInfo.getStageType() == StageInfo.StageType.LOCAL_FS)
                    ? null
                    : storageFactory.createClient(
                        stageInfo, parallel, encryptionMaterial.get(0), session),
                session,
                command,
                null,
                false,
                (parallel > 1 ? 1 : this.parallel),
                srcFileObj,
                encryptionMaterial.get(0),
                queryID));

        logger.debug("Submitted copy job for: {}", srcFile);
      }

      // shut down the thread executor
      threadExecutor.shutdown();

      try {
        // wait for all threads to complete without timeout
        threadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException ex) {
        throw new SnowflakeSQLLoggedException(
            queryID, session, ErrorCode.INTERRUPTED.getMessageCode(), SqlState.QUERY_CANCELED);
      }
      logger.debug("Done with uploading");

    } finally {
      // shut down the thread pool in any case
      if (threadExecutor != null) {
        threadExecutor.shutdownNow();
        threadExecutor = null;
      }
    }
  }

  private void segregateFilesBySize() {
    for (String srcFile : sourceFiles) {
      if ((new File(srcFile)).length() > bigFileThreshold) {
        if (bigSourceFiles == null) {
          bigSourceFiles = new HashSet<String>(sourceFiles.size());
        }

        bigSourceFiles.add(srcFile);
      } else {
        if (smallSourceFiles == null) {
          smallSourceFiles = new HashSet<String>(sourceFiles.size());
        }

        smallSourceFiles.add(srcFile);
      }
    }
  }

  public void cancel() {
    synchronized (canceled) {
      if (threadExecutor != null) {
        threadExecutor.shutdownNow();
        threadExecutor = null;
      }
      canceled = true;
    }
  }

  /**
   * process a list of file paths separated by "," and expand the wildcards if any to generate the
   * list of paths for all files matched by the wildcards
   *
   * @param filePathList file path list
   * @return a set of file names that is matched
   * @throws SnowflakeSQLException if cannot find the file
   */
  static Set<String> expandFileNames(String[] filePathList, String queryId)
      throws SnowflakeSQLException {
    Set<String> result = new HashSet<String>();

    // a location to file pattern map so that we only need to list the
    // same directory once when they appear in multiple times.
    Map<String, List<String>> locationToFilePatterns;

    locationToFilePatterns = new HashMap<String, List<String>>();

    String cwd = systemGetProperty("user.dir");

    for (String path : filePathList) {
      // replace ~ with user home
      if (path.startsWith("~")) {
        path = systemGetProperty("user.home") + path.substring(1);
      }

      // user may also specify files relative to current directory
      // add the current path if that is the case
      if (!(new File(path)).isAbsolute()) {
        logger.debug("Adding current working dir to relative file path.");

        path = cwd + localFSFileSep + path;
      }

      // check if the path contains any wildcards
      if (!path.contains("*")
          && !path.contains("?")
          && !(path.contains("[") && path.contains("]"))) {
        /* this file path doesn't have any wildcard, so we don't need to
         * expand it
         */
        result.add(path);
      } else {
        // get the directory path
        int lastFileSepIndex = path.lastIndexOf(localFSFileSep);

        // SNOW-15203: if we don't find a default file sep, try "/" if it is not
        // the default file sep.
        if (lastFileSepIndex < 0 && !"/".equals(localFSFileSep)) {
          lastFileSepIndex = path.lastIndexOf("/");
        }

        String loc = path.substring(0, lastFileSepIndex + 1);

        String filePattern = path.substring(lastFileSepIndex + 1);

        List<String> filePatterns = locationToFilePatterns.get(loc);

        if (filePatterns == null) {
          filePatterns = new ArrayList<String>();
          locationToFilePatterns.put(loc, filePatterns);
        }

        filePatterns.add(filePattern);
      }
    }

    // For each location, list files and match against the patterns
    for (Map.Entry<String, List<String>> entry : locationToFilePatterns.entrySet()) {
      try {
        File dir = new File(entry.getKey());

        logger.debug(
            "Listing files under: {} with patterns: {}",
            entry.getKey(),
            entry.getValue().toString());

        // Normal flow will never hit here. This is only for testing purposes
        if (isInjectedFileTransferExceptionEnabled()
            && injectedFileTransferException instanceof Exception) {
          throw (Exception) SnowflakeFileTransferAgent.injectedFileTransferException;
        }
        // The following currently ignore sub directories
        File[] filesMatchingPattern =
            dir.listFiles((FileFilter) new WildcardFileFilter(entry.getValue()));
        if (filesMatchingPattern != null) {
          for (File file : filesMatchingPattern) {
            result.add(file.getCanonicalPath());
          }
        } else {
          logger.debug("No files under {} matching pattern {}", entry.getKey(), entry.getValue());
        }
      } catch (Exception ex) {
        throw new SnowflakeSQLException(
            queryId,
            ex,
            SqlState.DATA_EXCEPTION,
            ErrorCode.FAIL_LIST_FILES.getMessageCode(),
            "Exception: "
                + ex.getMessage()
                + ", Dir="
                + entry.getKey()
                + ", Patterns="
                + entry.getValue().toString());
      }
    }

    logger.debug("Expanded file paths: ");

    for (String filePath : result) {
      logger.debug("File: {}", filePath);
    }

    return result;
  }

  private static boolean pushFileToLocal(
      String stageLocation,
      String filePath,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      SFBaseSession session,
      String queryId)
      throws SQLException {

    // replace ~ with user home
    stageLocation = stageLocation.replace("~", systemGetProperty("user.home"));
    try {
      logger.debug(
          "Copy file. srcFile: {}, destination: {}, destFileName: {}",
          filePath,
          stageLocation,
          destFileName);

      File destFile =
          new File(SnowflakeUtil.concatFilePathNames(stageLocation, destFileName, localFSFileSep));

      if (fileBackedOutStr != null) {
        inputStream = fileBackedOutStr.asByteSource().openStream();
      }
      FileUtils.copyInputStreamToFile(inputStream, destFile);
    } catch (Exception ex) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          ex.getMessage());
    }

    return true;
  }

  private static boolean pullFileFromLocal(
      String sourceLocation,
      String filePath,
      String destLocation,
      String destFileName,
      SFBaseSession session,
      String queryId)
      throws SQLException {
    try {
      logger.debug(
          "Copy file. srcFile: {}, destination: {}, destFileName: {}",
          sourceLocation + localFSFileSep + filePath,
          destLocation,
          destFileName);

      File srcFile =
          new File(SnowflakeUtil.concatFilePathNames(sourceLocation, filePath, localFSFileSep));

      FileUtils.copyFileToDirectory(srcFile, new File(destLocation));
    } catch (Exception ex) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          ex.getMessage());
    }

    return true;
  }

  private static void pushFileToRemoteStore(
      StageInfo stage,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      long uploadSize,
      String digest,
      FileCompressionType compressionType,
      SnowflakeStorageClient initialClient,
      SFSession session,
      String command,
      int parallel,
      File srcFile,
      boolean uploadFromStream,
      RemoteStoreFileEncryptionMaterial encMat,
      String streamingIngestClientName,
      String streamingIngestClientKey,
      String queryId)
      throws SQLException, IOException {
    remoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    String origDestFileName = destFileName;
    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
          remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.debug(
        "Upload object. Location: {}, key: {}, srcFile: {}, encryption: {}",
        remoteLocation.location,
        destFileName,
        srcFile,
        (ArgSupplier)
            () -> (encMat == null ? "NULL" : encMat.getSmkId() + "|" + encMat.getQueryId()));

    StorageObjectMetadata meta = storageFactory.createStorageMetadataObj(stage.getStageType());
    meta.setContentLength(uploadSize);
    if (digest != null) {
      initialClient.addDigestMetadata(meta, digest);
    }

    if (compressionType != null && compressionType.isSupported()) {
      meta.setContentEncoding(compressionType.name().toLowerCase());
    }

    if (streamingIngestClientName != null && streamingIngestClientKey != null) {
      initialClient.addStreamingIngestMetadata(
          meta, streamingIngestClientName, streamingIngestClientKey);
    }

    try {
      String presignedUrl = "";
      if (initialClient.requirePresignedUrl()) {
        // need to replace file://mypath/myfile?.csv with file://mypath/myfile1.csv.gz
        String localFilePath = getLocalFilePathFromCommand(command, false);
        String commandWithExactPath = command.replace(localFilePath, origDestFileName);
        // then hand that to GS to get the actual presigned URL we'll use
        SFStatement statement = new SFStatement(session);
        JsonNode jsonNode = parseCommandInGS(statement, commandWithExactPath);

        if (!jsonNode.path("data").path("stageInfo").path("presignedUrl").isMissingNode()) {
          presignedUrl = jsonNode.path("data").path("stageInfo").path("presignedUrl").asText();
        }
      }
      initialClient.upload(
          session,
          command,
          parallel,
          uploadFromStream,
          remoteLocation.location,
          srcFile,
          destFileName,
          inputStream,
          fileBackedOutStr,
          meta,
          stage.getRegion(),
          presignedUrl,
          queryId);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }

  /**
   * Static API function to upload data without JDBC session.
   *
   * <p>NOTE: This function is developed based on getUploadFileCallable().
   *
   * @param config Configuration to upload a file to cloud storage
   * @throws Exception if error occurs while data upload.
   */
  public static void uploadWithoutConnection(SnowflakeFileTransferConfig config) throws Exception {
    logger.trace("Entering uploadWithoutConnection...");

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) config.getSnowflakeFileTransferMetadata();
    InputStream uploadStream = config.getUploadStream();
    boolean requireCompress = config.getRequireCompress();
    int networkTimeoutInMilli = config.getNetworkTimeoutInMilli();
    OCSPMode ocspMode = config.getOcspMode();
    Properties proxyProperties = config.getProxyProperties();
    String streamingIngestClientName = config.getStreamingIngestClientName();
    String streamingIngestClientKey = config.getStreamingIngestClientKey();

    // Create HttpClient key
    HttpClientSettingsKey key =
        SnowflakeUtil.convertProxyPropertiesToHttpClientKey(ocspMode, proxyProperties);

    StageInfo stageInfo = metadata.getStageInfo();
    stageInfo.setProxyProperties(proxyProperties);
    String destFileName = metadata.getPresignedUrlFileName();

    logger.debug("Begin upload data for " + destFileName);

    long uploadSize;
    File fileToUpload = null;
    String digest = null;

    // Temp file that needs to be cleaned up when upload was successful
    FileBackedOutputStream fileBackedOutputStream = null;

    RemoteStoreFileEncryptionMaterial encMat = metadata.getEncryptionMaterial();
    if (encMat.getQueryId() == null
        && encMat.getQueryStageMasterKey() == null
        && encMat.getSmkId() == null) {
      encMat = null;
    }
    // SNOW-16082: we should capture exception if we fail to compress or
    // calculate digest.
    try {
      if (requireCompress) {
        InputStreamWithMetadata compressedSizeAndStream =
            (encMat == null
                ? compressStreamWithGZIPNoDigest(uploadStream, /* session= */ null, null)
                : compressStreamWithGZIP(uploadStream, /* session= */ null, encMat.getQueryId()));

        fileBackedOutputStream = compressedSizeAndStream.fileBackedOutputStream;

        // update the size
        uploadSize = compressedSizeAndStream.size;
        digest = compressedSizeAndStream.digest;

        if (compressedSizeAndStream.fileBackedOutputStream.getFile() != null) {
          fileToUpload = compressedSizeAndStream.fileBackedOutputStream.getFile();
        }

        logger.debug("New size after compression: {}", uploadSize);
      } else {
        // If it's not local_fs, we store our digest in the metadata
        // In local_fs, we don't need digest, and if we turn it on,
        // we will consume whole uploadStream, which local_fs uses.
        InputStreamWithMetadata result = computeDigest(uploadStream, true);
        digest = result.digest;
        fileBackedOutputStream = result.fileBackedOutputStream;
        uploadSize = result.size;

        if (result.fileBackedOutputStream.getFile() != null) {
          fileToUpload = result.fileBackedOutputStream.getFile();
        }
      }

      logger.debug(
          "Started copying file to {}:{} destName: {} compressed ? {} size={}",
          stageInfo.getStageType().name(),
          stageInfo.getLocation(),
          destFileName,
          (requireCompress ? "yes" : "no"),
          uploadSize);

      SnowflakeStorageClient initialClient =
          storageFactory.createClient(stageInfo, 1, encMat, /* session= */ null);

      // Normal flow will never hit here. This is only for testing purposes
      if (isInjectedFileTransferExceptionEnabled()) {
        throw (Exception) SnowflakeFileTransferAgent.injectedFileTransferException;
      }

      String queryId = encMat != null && encMat.getQueryId() != null ? encMat.getQueryId() : null;
      switch (stageInfo.getStageType()) {
        case S3:
        case AZURE:
          pushFileToRemoteStore(
              metadata.getStageInfo(),
              destFileName,
              uploadStream,
              fileBackedOutputStream,
              uploadSize,
              digest,
              (requireCompress ? FileCompressionType.GZIP : null),
              initialClient,
              config.getSession(),
              config.getCommand(),
              1,
              fileToUpload,
              (fileToUpload == null),
              encMat,
              streamingIngestClientName,
              streamingIngestClientKey,
              queryId);
          break;
        case GCS:
          // If the down-scoped token is used to upload file, one metadata may be used to upload
          // multiple files, so use the dest file name in config.
          destFileName =
              metadata.isForOneFile()
                  ? metadata.getPresignedUrlFileName()
                  : config.getDestFileName();

          pushFileToRemoteStoreWithPresignedUrl(
              metadata.getStageInfo(),
              destFileName,
              uploadStream,
              fileBackedOutputStream,
              uploadSize,
              digest,
              (requireCompress ? FileCompressionType.GZIP : null),
              initialClient,
              networkTimeoutInMilli,
              key,
              1,
              null,
              true,
              encMat,
              metadata.getPresignedUrl(),
              streamingIngestClientName,
              streamingIngestClientKey,
              queryId);
          break;
      }
    } catch (Exception ex) {
      logger.error("Exception encountered during file upload in uploadWithoutConnection", ex);
      throw ex;
    } finally {
      if (fileBackedOutputStream != null) {
        try {
          fileBackedOutputStream.reset();
        } catch (IOException ex) {
          logger.debug("Failed to clean up temp file: {}", ex);
        }
      }
    }
  }

  /**
   * Push a file (or stream) to remote store with pre-signed URL without JDBC session.
   *
   * <p>NOTE: This function is developed based on pushFileToRemoteStore(). The main difference is
   * that the caller needs to provide pre-signed URL and the upload doesn't need JDBC session.
   */
  private static void pushFileToRemoteStoreWithPresignedUrl(
      StageInfo stage,
      String destFileName,
      InputStream inputStream,
      FileBackedOutputStream fileBackedOutStr,
      long uploadSize,
      String digest,
      FileCompressionType compressionType,
      SnowflakeStorageClient initialClient,
      int networkTimeoutInMilli,
      HttpClientSettingsKey ocspModeAndProxyKey,
      int parallel,
      File srcFile,
      boolean uploadFromStream,
      RemoteStoreFileEncryptionMaterial encMat,
      String presignedUrl,
      String streamingIngestClientName,
      String streamingIngestClientKey,
      String queryId)
      throws SQLException, IOException {
    remoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    if (remoteLocation.path != null && !remoteLocation.path.isEmpty()) {
      destFileName =
          remoteLocation.path + (!remoteLocation.path.endsWith("/") ? "/" : "") + destFileName;
    }

    logger.debug(
        "Upload object. Location: {}, key: {}, srcFile: {}, encryption: {}",
        remoteLocation.location,
        destFileName,
        srcFile,
        (ArgSupplier)
            () -> (encMat == null ? "NULL" : encMat.getSmkId() + "|" + encMat.getQueryId()));

    StorageObjectMetadata meta = storageFactory.createStorageMetadataObj(stage.getStageType());
    meta.setContentLength(uploadSize);
    if (digest != null) {
      initialClient.addDigestMetadata(meta, digest);
    }

    if (compressionType != null && compressionType.isSupported()) {
      meta.setContentEncoding(compressionType.name().toLowerCase());
    }

    if (streamingIngestClientName != null && streamingIngestClientKey != null) {
      initialClient.addStreamingIngestMetadata(
          meta, streamingIngestClientName, streamingIngestClientKey);
    }

    try {
      initialClient.uploadWithPresignedUrlWithoutConnection(
          networkTimeoutInMilli,
          ocspModeAndProxyKey,
          parallel,
          uploadFromStream,
          remoteLocation.location,
          srcFile,
          destFileName,
          inputStream,
          fileBackedOutStr,
          meta,
          stage.getRegion(),
          presignedUrl,
          queryId);
    } finally {
      if (uploadFromStream && inputStream != null) {
        inputStream.close();
      }
    }
  }

  /**
   * This static method is called when we are handling an expired token exception It retrieves a
   * fresh token from GS and then calls .renew() on the storage client to refresh itself with the
   * new token
   *
   * @param session a session object
   * @param command a command to be retried
   * @param client a Snowflake Storage client object
   * @throws SnowflakeSQLException if any error occurs
   */
  public static void renewExpiredToken(
      SFSession session, String command, SnowflakeStorageClient client)
      throws SnowflakeSQLException {
    SFStatement statement = new SFStatement(session);
    JsonNode jsonNode = parseCommandInGS(statement, command);
    String queryId = jsonNode.path("data").path("queryId").asText();
    Map<?, ?> stageCredentials = extractStageCreds(jsonNode, queryId);

    // renew client with the fresh token
    logger.debug("Renewing expired access token");
    client.renew(stageCredentials);
  }

  private static void pullFileFromRemoteStore(
      StageInfo stage,
      String filePath,
      String destFileName,
      String localLocation,
      SnowflakeStorageClient initialClient,
      SFSession session,
      String command,
      int parallel,
      RemoteStoreFileEncryptionMaterial encMat,
      String presignedUrl,
      String queryId)
      throws SQLException {
    remoteLocation remoteLocation = extractLocationAndPath(stage.getLocation());

    String stageFilePath = filePath;

    if (!remoteLocation.path.isEmpty()) {
      stageFilePath = SnowflakeUtil.concatFilePathNames(remoteLocation.path, filePath, "/");
    }

    logger.debug(
        "Download object. Location: {}, key: {}, srcFile: {}, encryption: {}",
        remoteLocation.location,
        stageFilePath,
        filePath,
        (ArgSupplier)
            () -> (encMat == null ? "NULL" : encMat.getSmkId() + "|" + encMat.getQueryId()));

    initialClient.download(
        session,
        command,
        localLocation,
        destFileName,
        parallel,
        remoteLocation.location,
        stageFilePath,
        stage.getRegion(),
        presignedUrl,
        queryId);
  }

  /**
   * From the set of files intended to be uploaded/downloaded, derive a common prefix and use the
   * listObjects API to get the object summary for each object that has the common prefix.
   *
   * <p>For each returned object, we compare the size and digest with the local file and if they are
   * the same, we will not upload/download the file.
   *
   * @throws SnowflakeSQLException if any error occurs
   */
  private void filterExistingFiles() throws SnowflakeSQLException {
    /*
     * Build a reverse map from destination file name to source file path
     * The map will be used for looking up the source file for destination
     * files that already exist in destination location and mark them to be
     * skipped for uploading/downloading
     */
    Map<String, String> destFileNameToSrcFileMap =
        new HashMap<String, String>(fileMetadataMap.size());

    logger.debug("Build reverse map from destination file name to source file");

    for (Map.Entry<String, FileMetadata> entry : fileMetadataMap.entrySet()) {
      if (entry.getValue().destFileName != null) {
        String prevSrcFile =
            destFileNameToSrcFileMap.put(entry.getValue().destFileName, entry.getKey());

        if (prevSrcFile != null) {
          FileMetadata prevFileMetadata = fileMetadataMap.get(prevSrcFile);

          prevFileMetadata.resultStatus = ResultStatus.COLLISION;
          prevFileMetadata.errorDetails = prevSrcFile + " has same name as " + entry.getKey();
        }
      } else {
        logger.debug("No dest file name found for: {}", entry.getKey());
        logger.debug("Status: {}", entry.getValue().resultStatus);
      }
    }

    // no files to be processed
    if (destFileNameToSrcFileMap.size() == 0) {
      return;
    }

    // determine greatest common prefix for all stage file names so that
    // we can call remote store API to list the objects and get their digest to compare
    // with local files
    String[] stageFileNames;

    if (commandType == CommandType.UPLOAD) {
      stageFileNames = destFileNameToSrcFileMap.keySet().toArray(new String[0]);
    } else {
      stageFileNames = destFileNameToSrcFileMap.values().toArray(new String[0]);
    }

    // find greatest common prefix for all stage file names
    Arrays.sort(stageFileNames);

    String greatestCommonPrefix =
        SnowflakeUtil.greatestCommonPrefix(
            stageFileNames[0], stageFileNames[stageFileNames.length - 1]);

    logger.debug("Greatest common prefix: {}", greatestCommonPrefix);

    // use the greatest common prefix to list objects under stage location
    if (stageInfo.getStageType() == StageInfo.StageType.S3
        || stageInfo.getStageType() == StageInfo.StageType.AZURE
        || stageInfo.getStageType() == StageInfo.StageType.GCS) {
      logger.debug("Check existing files on remote storage for the common prefix");

      remoteLocation storeLocation = extractLocationAndPath(stageInfo.getLocation());

      StorageObjectSummaryCollection objectSummaries = null;

      int retryCount = 0;

      logger.debug("Start dragging object summaries from remote storage");
      do {
        try {
          // Normal flow will never hit here. This is only for testing purposes
          if (isInjectedFileTransferExceptionEnabled()
              && SnowflakeFileTransferAgent.injectedFileTransferException
                  instanceof StorageProviderException) {
            throw (StorageProviderException)
                SnowflakeFileTransferAgent.injectedFileTransferException;
          }
          objectSummaries =
              storageClient.listObjects(
                  storeLocation.location,
                  SnowflakeUtil.concatFilePathNames(storeLocation.path, greatestCommonPrefix, "/"));
          logger.debug("Received object summaries from remote storage");
        } catch (Exception ex) {
          logger.debug("Listing objects for filtering encountered exception: {}", ex.getMessage());

          // Need to unwrap StorageProviderException since handleStorageException only handle base
          // cause.
          if (ex instanceof StorageProviderException) {
            ex =
                (Exception)
                    ex.getCause(); // Cause of StorageProviderException is always an Exception
          }
          storageClient.handleStorageException(
              ex, ++retryCount, "listObjects", session, command, queryID);
          continue;
        }

        try {
          compareAndSkipRemoteFiles(objectSummaries, destFileNameToSrcFileMap);
          break; // exit retry loop
        } catch (Exception ex) {
          // This exception retry logic is mainly for Azure iterator. Since Azure iterator is a lazy
          // iterator,
          // it can throw exceptions during the for-each calls. To be more specific, iterator apis,
          // e.g. hasNext(), may throw Storage service error.
          logger.debug(
              "Comparison with existing files in remote storage encountered exception.", ex);
          if (ex instanceof StorageProviderException) {
            ex =
                (Exception)
                    ex.getCause(); // Cause of StorageProviderException is always an Exception
          }
          storageClient.handleStorageException(
              ex, ++retryCount, "compareRemoteFiles", session, command, queryID);
        }
      } while (retryCount <= storageClient.getMaxRetries());
    } else if (stageInfo.getStageType() == StageInfo.StageType.LOCAL_FS) {
      for (String stageFileName : stageFileNames) {
        String stageFilePath =
            SnowflakeUtil.concatFilePathNames(
                stageInfo.getLocation(), stageFileName, localFSFileSep);

        File stageFile = new File(stageFilePath);

        // if stage file doesn't exist, no need to skip whether for
        // upload/download
        if (!stageFile.exists()) {
          continue;
        }

        String mappedSrcFile =
            (commandType == CommandType.UPLOAD)
                ? destFileNameToSrcFileMap.get(stageFileName)
                : stageFileName;

        String localFile =
            (commandType == CommandType.UPLOAD)
                ? mappedSrcFile
                : (localLocation + fileMetadataMap.get(mappedSrcFile).destFileName);

        if (commandType == CommandType.UPLOAD
            && stageFileName.equals(fileMetadataMap.get(mappedSrcFile).destFileName)) {
          skipFile(mappedSrcFile, stageFileName);
          continue;
        }

        // Check file size first, if they are different, we don't need
        // to check digest
        if (!fileMetadataMap.get(mappedSrcFile).requireCompress
            && stageFile.length() != (new File(localFile)).length()) {
          logger.debug(
              "Size diff between stage and local, will {} {}",
              commandType.name().toLowerCase(),
              mappedSrcFile);
          continue;
        }

        // stage file exist and either we will be compressing or
        // the dest file has same size as the source file size we will
        // compare digest values below
        String localFileHashText = null;
        String stageFileHashText = null;

        List<FileBackedOutputStream> fileBackedOutputStreams = new ArrayList<>();
        InputStream localFileStream = null;
        try {
          // calculate the digest hash of the local file
          localFileStream = new FileInputStream(localFile);

          if (fileMetadataMap.get(mappedSrcFile).requireCompress) {
            logger.debug("Compressing stream for digest check");

            InputStreamWithMetadata res = compressStreamWithGZIP(localFileStream, session, queryID);
            fileBackedOutputStreams.add(res.fileBackedOutputStream);

            localFileStream = res.fileBackedOutputStream.asByteSource().openStream();
          }

          InputStreamWithMetadata res = computeDigest(localFileStream, false);
          localFileHashText = res.digest;
          fileBackedOutputStreams.add(res.fileBackedOutputStream);
        } catch (IOException | NoSuchAlgorithmException ex) {
          throw new SnowflakeSQLLoggedException(
              queryID,
              session,
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              ex,
              "Error reading local file: " + localFile);
        } finally {
          for (FileBackedOutputStream stream : fileBackedOutputStreams) {
            if (stream != null) {
              try {
                stream.reset();
              } catch (IOException ex) {
                logger.debug("Failed to clean up temp file: {}", ex);
              }
            }
          }
          IOUtils.closeQuietly(localFileStream);
        }

        FileBackedOutputStream fileBackedOutputStream = null;
        InputStream stageFileStream = null;
        try {
          // calculate digest for stage file
          stageFileStream = new FileInputStream(stageFilePath);

          InputStreamWithMetadata res = computeDigest(stageFileStream, false);
          stageFileHashText = res.digest;
          fileBackedOutputStream = res.fileBackedOutputStream;

        } catch (IOException | NoSuchAlgorithmException ex) {
          throw new SnowflakeSQLLoggedException(
              queryID,
              session,
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              ex,
              "Error reading stage file: " + stageFilePath);
        } finally {
          try {
            if (fileBackedOutputStream != null) {
              fileBackedOutputStream.reset();
            }
          } catch (IOException ex) {
            logger.debug("Failed to clean up temp file: {}", ex);
          }
          IOUtils.closeQuietly(stageFileStream);
        }

        // continue if digest is different so that we will process the file
        if (!stageFileHashText.equals(localFileHashText)) {
          logger.debug(
              "Digest diff between local and stage, will {} {}",
              commandType.name().toLowerCase(),
              mappedSrcFile);
          continue;
        } else {
          logger.debug("Digest matches between local and stage, will skip {}", mappedSrcFile);

          // skip the file given that the check sum is the same b/w source
          // and destination
          skipFile(mappedSrcFile, stageFileName);
        }
      }
    }
  }

  /**
   * For input objects, we compare the size and digest with the local files and if they are the
   * same, we will not upload/download the file.
   *
   * @param objectSummaries input objects collection
   * @param destFileNameToSrcFileMap map between dest file name and src file
   * @throws SnowflakeSQLException if any error occurs
   */
  private void compareAndSkipRemoteFiles(
      StorageObjectSummaryCollection objectSummaries, Map<String, String> destFileNameToSrcFileMap)
      throws SnowflakeSQLException {
    for (StorageObjectSummary obj : objectSummaries) {
      logger.debug(
          "Existing object: key: {} size: {} md5: {}", obj.getKey(), obj.getSize(), obj.getMD5());

      int idxOfLastFileSep = obj.getKey().lastIndexOf("/");
      String objFileName = obj.getKey().substring(idxOfLastFileSep + 1);

      // get the path to the local file so that we can calculate digest
      String mappedSrcFile = destFileNameToSrcFileMap.get(objFileName);

      // skip objects that don't have a corresponding file to be uploaded
      if (mappedSrcFile == null) {
        continue;
      }

      logger.debug(
          "Next compare digest for {} against {} on the remote store", mappedSrcFile, objFileName);

      String localFile = null;
      final boolean remoteEncrypted;

      try {
        // Normal flow will never hit here. This is only for testing purposes
        if (isInjectedFileTransferExceptionEnabled()) {
          throw (NoSuchAlgorithmException) SnowflakeFileTransferAgent.injectedFileTransferException;
        }

        localFile =
            (commandType == CommandType.UPLOAD) ? mappedSrcFile : (localLocation + objFileName);

        if (commandType == CommandType.DOWNLOAD && !(new File(localFile)).exists()) {
          logger.debug("File does not exist locally, will download {}", mappedSrcFile);
          continue;
        }

        // if it's an upload and there's already a file existing remotely with the same name, skip
        // uploading it
        if (commandType == CommandType.UPLOAD
            && objFileName.equals(fileMetadataMap.get(mappedSrcFile).destFileName)) {
          skipFile(mappedSrcFile, objFileName);
          continue;
        }

        // Check file size first, if their difference is bigger than the block
        // size, we don't need to compare digests
        if (!fileMetadataMap.get(mappedSrcFile).requireCompress
            && Math.abs(obj.getSize() - (new File(localFile)).length()) > 16) {
          logger.debug(
              "Size diff between remote and local, will {} {}",
              commandType.name().toLowerCase(),
              mappedSrcFile);
          continue;
        }

        // Get object metadata from remote storage
        //
        StorageObjectMetadata meta;

        try {
          meta = storageClient.getObjectMetadata(obj.getLocation(), obj.getKey());
        } catch (StorageProviderException spEx) {
          // SNOW-14521: when file is not found, ok to upload
          if (spEx.isServiceException404()) {
            // log it
            logger.debug(
                "File returned from listing but found missing {} when getting its"
                    + " metadata. Location: {}, key: {}",
                obj.getLocation(),
                obj.getKey());

            // the file is not found, ok to upload
            continue;
          }

          // for any other exception, log an error
          logger.error("Fetching object metadata encountered exception: {}", spEx.getMessage());

          throw spEx;
        }

        String objDigest = storageClient.getDigestMetadata(meta);

        remoteEncrypted =
            MatDesc.parse(meta.getUserMetadata().get(storageClient.getMatdescKey())) != null;

        // calculate the digest hash of the local file
        InputStream fileStream = null;
        String hashText = null;

        // Streams (potentially with temp files) to clean up
        final List<FileBackedOutputStream> fileBackedOutputStreams = new ArrayList<>();
        try {
          fileStream = new FileInputStream(localFile);
          if (fileMetadataMap.get(mappedSrcFile).requireCompress) {
            logger.debug("Compressing stream for digest check");

            InputStreamWithMetadata res = compressStreamWithGZIP(fileStream, session, queryID);

            fileStream = res.fileBackedOutputStream.asByteSource().openStream();
            fileBackedOutputStreams.add(res.fileBackedOutputStream);
          }

          // If the remote file has our digest, compute the SHA-256
          // for the local file
          // If the remote file does not have our digest but is unencrypted,
          // we compare the MD5 of the unencrypted local file to the ETag
          // of the S3 file.
          // Otherwise (remote file is encrypted, but has no sfc-digest),
          // no comparison is performed
          if (objDigest != null) {
            InputStreamWithMetadata res = computeDigest(fileStream, false);
            hashText = res.digest;
            fileBackedOutputStreams.add(res.fileBackedOutputStream);

          } else if (!remoteEncrypted) {
            hashText = DigestUtils.md5Hex(fileStream);
          }
        } finally {
          if (fileStream != null) {
            fileStream.close();
          }

          for (FileBackedOutputStream stream : fileBackedOutputStreams) {
            if (stream != null) {
              try {
                stream.reset();
              } catch (IOException ex) {
                logger.debug("Failed to clean up temp file: {}", ex);
              }
            }
          }
        }

        // continue so that we will upload the file
        if (hashText == null
            || // remote is encrypted & has no digest
            (objDigest != null && !hashText.equals(objDigest))
            || // digest mismatch
            (objDigest == null && !hashText.equals(obj.getMD5()))) // ETag/MD5 mismatch
        {
          logger.debug(
              "Digest diff between remote store and local, will {} {}, "
                  + "local digest: {}, remote store md5: {}",
              commandType.name().toLowerCase(),
              mappedSrcFile,
              hashText,
              obj.getMD5());
          continue;
        }
      } catch (IOException | NoSuchAlgorithmException ex) {
        throw new SnowflakeSQLLoggedException(
            queryID,
            session,
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            ex,
            "Error reading: " + localFile);
      }

      logger.debug(
          "Digest same between remote store and local, will not upload {} {}",
          commandType.name().toLowerCase(),
          mappedSrcFile);

      skipFile(mappedSrcFile, objFileName);
    }
  }

  private void skipFile(String srcFilePath, String destFileName) {
    FileMetadata fileMetadata = fileMetadataMap.get(srcFilePath);

    if (fileMetadata != null) {
      if (fileMetadata.resultStatus == null || fileMetadata.resultStatus == ResultStatus.UNKNOWN) {
        logger.debug("Mark {} as skipped", srcFilePath);

        fileMetadata.resultStatus = ResultStatus.SKIPPED;
        fileMetadata.errorDetails =
            "File with same destination name and checksum already exists: " + destFileName;
      } else {
        logger.debug(
            "No need to mark as skipped for: {} status was already marked as: {}",
            srcFilePath,
            fileMetadata.resultStatus);
      }
    }
  }

  private void initFileMetadata() throws SnowflakeSQLException {
    // file metadata is keyed on source file names (which are local file names
    // for upload command and stage file names for download command)
    fileMetadataMap = new HashMap<String, FileMetadata>(sourceFiles.size());

    if (commandType == CommandType.UPLOAD) {
      if (sourceFromStream) {
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadataMap.put(SRC_FILE_NAME_FOR_STREAM, fileMetadata);
        fileMetadata.srcFileName = SRC_FILE_NAME_FOR_STREAM;
      } else {
        for (String sourceFile : sourceFiles) {
          FileMetadata fileMetadata = new FileMetadata();
          fileMetadataMap.put(sourceFile, fileMetadata);
          File file = new File(sourceFile);

          fileMetadata.srcFileName = file.getName();
          fileMetadata.srcFileSize = file.length();

          if (!file.exists()) {
            logger.debug("File doesn't exist: {}", sourceFile);

            throw new SnowflakeSQLLoggedException(
                queryID,
                session,
                ErrorCode.FILE_NOT_FOUND.getMessageCode(),
                SqlState.DATA_EXCEPTION,
                sourceFile);
          } else if (file.isDirectory()) {
            logger.debug("Not a file, but directory: {}", sourceFile);

            throw new SnowflakeSQLLoggedException(
                queryID,
                session,
                ErrorCode.FILE_IS_DIRECTORY.getMessageCode(),
                SqlState.DATA_EXCEPTION,
                sourceFile);
          }
        }
      }
    } else if (commandType == CommandType.DOWNLOAD) {
      for (String sourceFile : sourceFiles) {
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadataMap.put(sourceFile, fileMetadata);
        fileMetadata.srcFileName = sourceFile;

        fileMetadata.destFileName =
            sourceFile.substring(sourceFile.lastIndexOf("/") + 1); // s3 uses / as separator
      }
    }
  }

  /**
   * Derive compression type from mime type
   *
   * @param mimeTypeStr The mime type passed to us
   * @return the Optional for the compression type or Optional.empty()
   */
  static Optional<FileCompressionType> mimeTypeToCompressionType(String mimeTypeStr) {
    if (mimeTypeStr == null) {
      return Optional.empty();
    }
    int slashIndex = mimeTypeStr.indexOf('/');
    if (slashIndex < 0) {
      return Optional.empty(); // unable to find sub type
    }
    int semiColonIndex = mimeTypeStr.indexOf(';');
    String subType;
    if (semiColonIndex < 0) {
      subType = mimeTypeStr.substring(slashIndex + 1).trim().toLowerCase(Locale.ENGLISH);
    } else {
      subType = mimeTypeStr.substring(slashIndex + 1, semiColonIndex);
    }
    if (Strings.isNullOrEmpty(subType)) {
      return Optional.empty();
    }
    return FileCompressionType.lookupByMimeSubType(subType);
  }

  /**
   * Detect file compression type for all files to be uploaded
   *
   * @throws SnowflakeSQLException Will be thrown if the compression type is unknown or unsupported
   */
  private void processFileCompressionTypes() throws SnowflakeSQLException {
    // see what user has told us about the source file compression types
    boolean autoDetect = true;
    FileCompressionType userSpecifiedSourceCompression = null;

    if (SOURCE_COMPRESSION_AUTO_DETECT.equalsIgnoreCase(sourceCompression)) {
      autoDetect = true;
    } else if (SOURCE_COMPRESSION_NONE.equalsIgnoreCase(sourceCompression)) {
      autoDetect = false;
    } else {
      Optional<FileCompressionType> foundCompType =
          FileCompressionType.lookupByMimeSubType(sourceCompression.toLowerCase());
      if (!foundCompType.isPresent()) {
        throw new SnowflakeSQLLoggedException(
            queryID,
            session,
            ErrorCode.COMPRESSION_TYPE_NOT_KNOWN.getMessageCode(),
            SqlState.FEATURE_NOT_SUPPORTED,
            sourceCompression);
      }
      userSpecifiedSourceCompression = foundCompType.get();

      if (!userSpecifiedSourceCompression.isSupported()) {
        throw new SnowflakeSQLLoggedException(
            queryID,
            session,
            ErrorCode.COMPRESSION_TYPE_NOT_SUPPORTED.getMessageCode(),
            SqlState.FEATURE_NOT_SUPPORTED,
            sourceCompression);
      }

      autoDetect = false;
    }

    if (!sourceFromStream) {
      for (String srcFile : sourceFiles) {
        FileMetadata fileMetadata = fileMetadataMap.get(srcFile);

        if (fileMetadata.resultStatus == ResultStatus.NONEXIST
            || fileMetadata.resultStatus == ResultStatus.DIRECTORY) {
          continue;
        }

        File file = new File(srcFile);
        String srcFileName = file.getName();

        String mimeTypeStr = null;
        FileCompressionType currentFileCompressionType = null;

        try {
          if (autoDetect) {
            // probe the file for compression type using tika file type detector
            mimeTypeStr = Files.probeContentType(file.toPath());

            if (mimeTypeStr == null) {
              try (FileInputStream f = new FileInputStream(file)) {
                byte[] magic = new byte[4];
                if (f.read(magic, 0, 4) == 4) {
                  if (Arrays.equals(magic, new byte[] {'P', 'A', 'R', '1'})) {
                    mimeTypeStr = "snowflake/parquet";
                  } else if (Arrays.equals(
                      Arrays.copyOfRange(magic, 0, 3), new byte[] {'O', 'R', 'C'})) {
                    mimeTypeStr = "snowflake/orc";
                  }
                }
              }
            }

            if (mimeTypeStr != null) {
              logger.debug("Mime type for {} is: {}", srcFile, mimeTypeStr);

              Optional<FileCompressionType> foundCompType = mimeTypeToCompressionType(mimeTypeStr);
              if (foundCompType.isPresent()) {
                currentFileCompressionType = foundCompType.get();
              }
            }

            // fallback: use file extension
            if (currentFileCompressionType == null) {
              mimeTypeStr = getMimeTypeFromFileExtension(srcFile);

              if (mimeTypeStr != null) {
                logger.debug("Mime type for {} is: {}", srcFile, mimeTypeStr);
                Optional<FileCompressionType> foundCompType =
                    mimeTypeToCompressionType(mimeTypeStr);
                if (foundCompType.isPresent()) {
                  currentFileCompressionType = foundCompType.get();
                }
              }
            }
          } else {
            currentFileCompressionType = userSpecifiedSourceCompression;
          }

          // check if the compression type is supported by us
          if (currentFileCompressionType != null) {
            fileMetadata.srcCompressionType = currentFileCompressionType;

            if (currentFileCompressionType.isSupported()) {
              // remember the compression type if supported
              fileMetadata.destCompressionType = currentFileCompressionType;
              fileMetadata.requireCompress = false;
              fileMetadata.destFileName = srcFileName;
              logger.debug(
                  "File compression detected as {} for: {}",
                  currentFileCompressionType.name(),
                  srcFile);
            } else {
              // error if not supported
              throw new SnowflakeSQLLoggedException(
                  queryID,
                  session,
                  ErrorCode.COMPRESSION_TYPE_NOT_SUPPORTED.getMessageCode(),
                  SqlState.FEATURE_NOT_SUPPORTED,
                  currentFileCompressionType.name());
            }
          } else {
            // we want to auto compress the files unless the user has disabled it
            logger.debug("Compression not found for file: {}", srcFile);

            // Set compress flag
            fileMetadata.requireCompress = autoCompress;
            fileMetadata.srcCompressionType = null;

            if (autoCompress) {
              // We only support gzip auto compression
              fileMetadata.destFileName = srcFileName + FileCompressionType.GZIP.getFileExtension();
              fileMetadata.destCompressionType = FileCompressionType.GZIP;
            } else {
              fileMetadata.destFileName = srcFileName;
              fileMetadata.destCompressionType = null;
            }
          }
        } catch (Exception ex) {

          // SNOW-13146: don't log severe message for user error
          if (ex instanceof SnowflakeSQLException) {
            logger.debug("Exception encountered when processing file compression types", ex);
          } else {
            logger.debug("Exception encountered when processing file compression types", ex);
          }

          fileMetadata.resultStatus = ResultStatus.ERROR;
          fileMetadata.errorDetails = ex.getMessage();
        }
      }
    } else {
      // source from stream case
      FileMetadata fileMetadata = fileMetadataMap.get(SRC_FILE_NAME_FOR_STREAM);
      fileMetadata.srcCompressionType = userSpecifiedSourceCompression;

      if (compressSourceFromStream) {
        fileMetadata.destCompressionType = FileCompressionType.GZIP;
        fileMetadata.requireCompress = true;
      } else {
        fileMetadata.destCompressionType = userSpecifiedSourceCompression;
        fileMetadata.requireCompress = false;
      }

      // add gz extension if file name doesn't have it
      if (compressSourceFromStream
          && !destFileNameForStreamSource.endsWith(FileCompressionType.GZIP.getFileExtension())) {
        fileMetadata.destFileName =
            destFileNameForStreamSource + FileCompressionType.GZIP.getFileExtension();
      } else {
        fileMetadata.destFileName = destFileNameForStreamSource;
      }
    }
  }

  /**
   * Derive mime type from file extension
   *
   * @param srcFile The source file name
   * @return the mime type derived from the file extension
   */
  private String getMimeTypeFromFileExtension(String srcFile) {
    String srcFileLowCase = srcFile.toLowerCase();

    for (FileCompressionType compressionType : FileCompressionType.values()) {
      if (srcFileLowCase.endsWith(compressionType.getFileExtension())) {
        return compressionType.getMimeType() + "/" + compressionType.getMimeSubTypes().get(0);
      }
    }

    return null;
  }

  /**
   * A small helper for extracting location name and path from full location path
   *
   * @param stageLocationPath stage location
   * @return remoteLocation object
   */
  public static remoteLocation extractLocationAndPath(String stageLocationPath) {
    String location = stageLocationPath;
    String path = "";

    // split stage location as location name and path
    if (stageLocationPath.contains("/")) {
      location = stageLocationPath.substring(0, stageLocationPath.indexOf("/"));
      path = stageLocationPath.substring(stageLocationPath.indexOf("/") + 1);
    }

    return new remoteLocation(location, path);
  }

  /** Generate status rows for each file */
  private void populateStatusRows() {
    for (Map.Entry<String, FileMetadata> entry : fileMetadataMap.entrySet()) {
      FileMetadata fileMetadata = entry.getValue();

      if (commandType == CommandType.UPLOAD) {
        statusRows.add(
            showEncryptionParameter
                ? new UploadCommandEncryptionFacade(
                    fileMetadata.srcFileName,
                    fileMetadata.destFileName,
                    fileMetadata.resultStatus.name(),
                    fileMetadata.errorDetails,
                    fileMetadata.srcFileSize,
                    fileMetadata.destFileSize,
                    (fileMetadata.srcCompressionType == null)
                        ? "NONE"
                        : fileMetadata.srcCompressionType.name(),
                    (fileMetadata.destCompressionType == null)
                        ? "NONE"
                        : fileMetadata.destCompressionType.name(),
                    fileMetadata.isEncrypted)
                : new UploadCommandFacade(
                    fileMetadata.srcFileName,
                    fileMetadata.destFileName,
                    fileMetadata.resultStatus.name(),
                    fileMetadata.errorDetails,
                    fileMetadata.srcFileSize,
                    fileMetadata.destFileSize,
                    (fileMetadata.srcCompressionType == null)
                        ? "NONE"
                        : fileMetadata.srcCompressionType.name(),
                    (fileMetadata.destCompressionType == null)
                        ? "NONE"
                        : fileMetadata.destCompressionType.name()));
      } else if (commandType == CommandType.DOWNLOAD) {
        statusRows.add(
            showEncryptionParameter
                ? new DownloadCommandEncryptionFacade(
                    fileMetadata.srcFileName.startsWith("/")
                        ? fileMetadata.srcFileName.substring(1)
                        : fileMetadata.srcFileName,
                    fileMetadata.resultStatus.name(),
                    fileMetadata.errorDetails,
                    fileMetadata.destFileSize,
                    fileMetadata.isEncrypted)
                : new DownloadCommandFacade(
                    fileMetadata.srcFileName.startsWith("/")
                        ? fileMetadata.srcFileName.substring(1)
                        : fileMetadata.srcFileName,
                    fileMetadata.resultStatus.name(),
                    fileMetadata.errorDetails,
                    fileMetadata.destFileSize));
      }
    }

    /* we sort the result if the connection is in sorting mode
     */
    Object sortProperty = null;

    sortProperty = session.getSessionPropertyByKey("sort");

    boolean sortResult = sortProperty != null && (Boolean) sortProperty;

    if (sortResult) {
      Comparator<Object> comparator =
          (commandType == CommandType.UPLOAD)
              ? new Comparator<Object>() {
                public int compare(Object a, Object b) {
                  String srcFileNameA = ((UploadCommandFacade) a).getSrcFile();
                  String srcFileNameB = ((UploadCommandFacade) b).getSrcFile();

                  return srcFileNameA.compareTo(srcFileNameB);
                }
              }
              : new Comparator<Object>() {
                public int compare(Object a, Object b) {
                  String srcFileNameA = ((DownloadCommandFacade) a).getFile();
                  String srcFileNameB = ((DownloadCommandFacade) b).getFile();

                  return srcFileNameA.compareTo(srcFileNameB);
                }
              };

      // sort the rows by source file names
      Collections.sort(statusRows, comparator);
    }
  }

  public Object getResultSet() throws SnowflakeSQLException {
    return new SFFixedViewResultSet(this, this.commandType, this.queryID);
  }

  public CommandType getCommandType() {
    return commandType;
  }

  /**
   * Handles an InvalidKeyException which indicates that the JCE component is not installed properly
   *
   * @deprecated use {@link #throwJCEMissingError(String, Exception, String)}
   * @param operation a string indicating the operation type, e.g. upload/download
   * @param ex The exception to be handled
   * @throws SnowflakeSQLException throws the error as a SnowflakeSQLException
   */
  @Deprecated
  public static void throwJCEMissingError(String operation, Exception ex)
      throws SnowflakeSQLException {
    throwJCEMissingError(operation, ex, null);
  }

  /**
   * Handles an InvalidKeyException which indicates that the JCE component is not installed properly
   *
   * @param operation a string indicating the operation type, e.g. upload/download
   * @param ex The exception to be handled
   * @param queryId last query id if available
   * @throws SnowflakeSQLException throws the error as a SnowflakeSQLException
   */
  public static void throwJCEMissingError(String operation, Exception ex, String queryId)
      throws SnowflakeSQLException {
    // Most likely cause: Unlimited strength policy files not installed
    String msg =
        "Strong encryption with Java JRE requires JCE "
            + "Unlimited Strength Jurisdiction Policy files. "
            + "Follow JDBC client installation instructions "
            + "provided by Snowflake or contact Snowflake Support.";

    logger.error(
        "JCE Unlimited Strength policy files missing: {}. {}.",
        ex.getMessage(),
        ex.getCause().getMessage());

    String bootLib = systemGetProperty("sun.boot.library.path");
    if (bootLib != null) {
      msg +=
          " The target directory on your system is: " + Paths.get(bootLib, "security").toString();
      logger.error(msg);
    }
    throw new SnowflakeSQLException(
        queryId,
        ex,
        SqlState.SYSTEM_ERROR,
        ErrorCode.AWS_CLIENT_ERROR.getMessageCode(),
        operation,
        msg);
  }

  /**
   * For handling IOException: No space left on device when attempting to download a file to a
   * location where there is not enough space. We don't want to retry on this exception.
   *
   * @deprecated use {@link #throwNoSpaceLeftError(SFSession, String, Exception, String)}
   * @param session the current session
   * @param operation the operation i.e. GET
   * @param ex the exception caught
   * @throws SnowflakeSQLLoggedException if not enough space left on device to download file.
   */
  @Deprecated
  public static void throwNoSpaceLeftError(SFSession session, String operation, Exception ex)
      throws SnowflakeSQLLoggedException {
    throwNoSpaceLeftError(session, operation, ex, null);
  }

  /**
   * For handling IOException: No space left on device when attempting to download a file to a
   * location where there is not enough space. We don't want to retry on this exception.
   *
   * @param session the current session
   * @param operation the operation i.e. GET
   * @param ex the exception caught
   * @param queryId the query ID
   * @throws SnowflakeSQLLoggedException if not enough space left on device to download file.
   */
  public static void throwNoSpaceLeftError(
      SFSession session, String operation, Exception ex, String queryId)
      throws SnowflakeSQLLoggedException {
    String exMessage = SnowflakeUtil.getRootCause(ex).getMessage();
    if (exMessage != null && exMessage.equals(NO_SPACE_LEFT_ON_DEVICE_ERR)) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.SYSTEM_ERROR,
          ErrorCode.IO_ERROR.getMessageCode(),
          ex,
          "Encountered exception during " + operation + ": " + ex.getMessage());
    }
  }
}
