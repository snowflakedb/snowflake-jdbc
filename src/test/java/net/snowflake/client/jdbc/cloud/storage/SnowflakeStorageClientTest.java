package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FileBackedOutputStream;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SnowflakeStorageClientTest {

  private static SnowflakeStorageClient storageClient;

  @BeforeAll
  public static void setUp() {
    // Mock an implementation of the interface
    storageClient =
        new SnowflakeStorageClient() {
          @Override
          public int getMaxRetries() {
            return 0;
          }

          @Override
          public int getRetryBackoffMaxExponent() {
            return 0;
          }

          @Override
          public int getRetryBackoffMin() {
            return 0;
          }

          @Override
          public boolean isEncrypting() {
            return false;
          }

          @Override
          public int getEncryptionKeySize() {
            return 0;
          }

          @Override
          public void renew(Map<?, ?> stageCredentials) {}

          @Override
          public void shutdown() {}

          @Override
          public StorageObjectSummaryCollection listObjects(
              String remoteStorageLocation, String prefix) {
            return null;
          }

          @Override
          public StorageObjectMetadata getObjectMetadata(
              String remoteStorageLocation, String prefix) {
            return null;
          }

          @Override
          public void download(
              SFSession connection,
              String command,
              String localLocation,
              String destFileName,
              int parallelism,
              String remoteStorageLocation,
              String stageFilePath,
              String stageRegion,
              String presignedUrl,
              String queryId) {}

          @Override
          public InputStream downloadToStream(
              SFSession connection,
              String command,
              int parallelism,
              String remoteStorageLocation,
              String stageFilePath,
              String stageRegion,
              String presignedUrl,
              String queryId) {
            return null;
          }

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
              String stageRegion,
              String presignedUrl,
              String queryId) {}

          @Override
          public void handleStorageException(
              Exception ex,
              int retryCount,
              String operation,
              SFSession connection,
              String command,
              String queryId) {}

          @Override
          public String getMatdescKey() {
            return null;
          }

          @Override
          public void addEncryptionMetadata(
              StorageObjectMetadata meta,
              MatDesc matDesc,
              byte[] ivData,
              byte[] encryptedKey,
              long contentLength) {}

          @Override
          public void addDigestMetadata(StorageObjectMetadata meta, String digest) {}

          @Override
          public String getDigestMetadata(StorageObjectMetadata meta) {
            return null;
          }

          @Override
          public void addStreamingIngestMetadata(
              StorageObjectMetadata meta, String clientName, String clientKey) {}

          @Override
          public String getStreamingIngestClientName(StorageObjectMetadata meta) {
            return null;
          }

          @Override
          public String getStreamingIngestClientKey(StorageObjectMetadata meta) {
            return null;
          }
        };
  }

  @Test
  void testRequirePresignedUrl_DefaultReturnsFalse() {
    assertFalse(
        storageClient.requirePresignedUrl(), "Default requirePresignedUrl should return false.");
  }

  @Test
  void testDownload_DefaultMethodCallsOverloadedMethod() {
    SFSession session = mock(SFSession.class);
    assertDoesNotThrow(
        () ->
            storageClient.download(
                session,
                "command",
                "localPath",
                "destFile",
                4,
                "remoteLocation",
                "stagePath",
                "stageRegion",
                "presignedUrl"));
  }

  @Test
  void testDownloadToStream_DefaultMethodCallsOverloadedMethod() {
    SFSession session = mock(SFSession.class);
    assertDoesNotThrow(
        () ->
            storageClient.downloadToStream(
                session,
                "command",
                4,
                "remoteLocation",
                "stagePath",
                "stageRegion",
                "presignedUrl"));
  }

  @Test
  void testUpload_DefaultMethodCallsOverloadedMethod() {
    SFSession session = mock(SFSession.class);
    assertDoesNotThrow(
        () ->
            storageClient.upload(
                session,
                "command",
                4,
                true,
                "remoteLocation",
                new File("test.txt"),
                "destFile",
                null,
                null,
                null,
                "stageRegion",
                "presignedUrl"));
  }

  @Test
  void testUploadWithPresignedUrlWithoutConnection_ThrowsExceptionWhenNotPresignedUrl() {
    SnowflakeSQLLoggedException exception =
        assertThrows(
            SnowflakeSQLLoggedException.class,
            () ->
                storageClient.uploadWithPresignedUrlWithoutConnection(
                    5000,
                    mock(HttpClientSettingsKey.class),
                    4,
                    true,
                    "remoteLocation",
                    new File("test.txt"),
                    "destFile",
                    null,
                    null,
                    null,
                    "stageRegion",
                    "presignedUrl",
                    "queryId"));
    assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), exception.getErrorCode());
  }

  @Test
  void testHandleStorageException_DefaultMethodCallsOverloadedMethod() {
    SFSession session = mock(SFSession.class);
    Exception exception = new Exception("Test exception");
    assertDoesNotThrow(
        () -> storageClient.handleStorageException(exception, 3, "upload", session, "command"));
  }

  @Test
  void testAddEncryptionMetadataForGcm_NoExceptionThrown() {
    StorageObjectMetadata meta = mock(StorageObjectMetadata.class);
    MatDesc matDesc = mock(MatDesc.class);
    byte[] encryptedKey = new byte[16];
    byte[] dataIvBytes = new byte[12];
    byte[] keyIvBytes = new byte[12];
    byte[] keyAad = new byte[16];
    byte[] dataAad = new byte[16];

    assertDoesNotThrow(
        () ->
            storageClient.addEncryptionMetadataForGcm(
                meta, matDesc, encryptedKey, dataIvBytes, keyIvBytes, keyAad, dataAad, 100L));
  }
}
