package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.S3StorageObjectMetadata;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeGCSClient;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeStorageClient;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageClientFactory;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.cloud.storage.StorageProviderException;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for SnowflakeFileTransferAgent that require an active connection */
@Tag(TestTags.OTHERS)
public class FileUploaderLatestIT extends FileUploaderPrep {
  private static final String OBJ_META_STAGE = "testObjMeta";
  private ObjectMapper mapper = new ObjectMapper();
  private static final String PUT_COMMAND = "put file:///dummy/path/file2.gz @testStage";

  /**
   * This tests that getStageInfo(JsonNode, session) reflects the boolean value of UseS3RegionalUrl
   * that has been set via the session.
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testGetS3StageDataWithS3Session() throws SQLException {
    try (Connection con = getConnection("s3testaccount")) {
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      // Set UseRegionalS3EndpointsForPresignedURL to true in session
      sfSession.setUseRegionalS3EndpointsForPresignedURL(true);

      // Get sample stage info with session
      StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleS3JsonNode, sfSession);
      assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
      // Assert that true value from session is reflected in StageInfo
      assertEquals(true, stageInfo.getUseS3RegionalUrl());

      // Set UseRegionalS3EndpointsForPresignedURL to false in session
      sfSession.setUseRegionalS3EndpointsForPresignedURL(false);
      stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleS3JsonNode, sfSession);
      assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
      // Assert that false value from session is reflected in StageInfo
      assertEquals(false, stageInfo.getUseS3RegionalUrl());
    }
  }

  /**
   * This tests that setting the value of UseS3RegionalUrl for a non-S3 account session has no
   * effect on the function getStageInfo(JsonNode, session).
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testGetS3StageDataWithAzureSession() throws SQLException {
    try (Connection con = getConnection("azureaccount")) {
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      // Set UseRegionalS3EndpointsForPresignedURL to true in session. This is redundant since
      // session
      // is Azure
      sfSession.setUseRegionalS3EndpointsForPresignedURL(true);

      // Get sample stage info with session
      StageInfo stageInfo =
          SnowflakeFileTransferAgent.getStageInfo(exampleAzureJsonNode, sfSession);
      assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
      assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
      // Assert that UseRegionalS3EndpointsForPresignedURL is false in StageInfo even if it was set
      // to
      // true.
      // The value should always be false for non-S3 accounts
      assertEquals(false, stageInfo.getUseS3RegionalUrl());
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetObjectMetadataWithGCS() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
        String putCommand = "PUT file://" + sourceFilePath + " @" + OBJ_META_STAGE;
        statement.execute(putCommand);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
        StageInfo info = sfAgent.getStageInfo();
        SnowflakeGCSClient client =
            SnowflakeGCSClient.createSnowflakeGCSClient(
                info, sfAgent.getEncryptionMaterial().get(0), sfSession);

        String location = info.getLocation();
        int idx = location.indexOf('/');
        String remoteStageLocation = location.substring(0, idx);
        String path = location.substring(idx + 1) + TEST_DATA_FILE + ".gz";
        StorageObjectMetadata metadata = client.getObjectMetadata(remoteStageLocation, path);
        assertEquals("gzip", metadata.getContentEncoding());
      } finally {
        statement.execute("DROP STAGE if exists " + OBJ_META_STAGE);
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetObjectMetadataFileNotFoundWithGCS() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
        String putCommand = "PUT file://" + sourceFilePath + " @" + OBJ_META_STAGE;
        statement.execute(putCommand);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
        StageInfo info = sfAgent.getStageInfo();
        SnowflakeGCSClient client =
            SnowflakeGCSClient.createSnowflakeGCSClient(
                info, sfAgent.getEncryptionMaterial().get(0), sfSession);

        String location = info.getLocation();
        int idx = location.indexOf('/');
        String remoteStageLocation = location.substring(0, idx);
        String path = location.substring(idx + 1) + "wrong_file.csv.gz";
        StorageProviderException thrown =
            assertThrows(
                StorageProviderException.class,
                () -> client.getObjectMetadata(remoteStageLocation, path));
        assertTrue(thrown.getMessage().matches(".*Blob.*not found in bucket.*"));
      } finally {
        statement.execute("DROP STAGE if exists " + OBJ_META_STAGE);
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetObjectMetadataStorageExceptionWithGCS() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
        String putCommand = "PUT file://" + sourceFilePath + " @" + OBJ_META_STAGE;
        statement.execute(putCommand);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
        StageInfo info = sfAgent.getStageInfo();
        SnowflakeGCSClient client =
            SnowflakeGCSClient.createSnowflakeGCSClient(
                info, sfAgent.getEncryptionMaterial().get(0), sfSession);

        String location = info.getLocation();
        int idx = location.indexOf('/');
        String remoteStageLocation = location.substring(0, idx);
        StorageProviderException thrown =
            assertThrows(
                StorageProviderException.class,
                () -> client.getObjectMetadata(remoteStageLocation, ""));
        assertTrue(thrown.getMessage().matches(".*Permission.*denied.*"));
      } finally {
        statement.execute("DROP STAGE if exists " + OBJ_META_STAGE);
      }
    }
  }

  @Test
  public void testGetFileTransferCommandType() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));
        assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, sfAgent.getCommandType());
      } finally {
        statement.execute("drop stage if exists testStage");
      }
    }
  }

  @Test
  public void testNullCommand() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeSQLException thrown =
            assertThrows(
                SnowflakeSQLException.class,
                () -> new SnowflakeFileTransferAgent(null, sfSession, new SFStatement(sfSession)));
        assertTrue(
            thrown
                .getMessage()
                .contains("JDBC driver internal error: Missing sql for statement execution"));
      } finally {
        statement.execute("drop stage if exists testStage");
      }
    }
  }

  @Test
  public void testCompressStreamWithGzipException() throws Exception {
    // inject the NoSuchAlgorithmException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new NoSuchAlgorithmException());

    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

        List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
        SnowflakeFileTransferMetadataV1 metadata =
            (SnowflakeFileTransferMetadataV1) metadataList.get(0);

        String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
        InputStream inputStream = new FileInputStream(srcPath);
        SnowflakeSQLException thrown =
            assertThrows(
                SnowflakeSQLException.class,
                () ->
                    SnowflakeFileTransferAgent.uploadWithoutConnection(
                        SnowflakeFileTransferConfig.Builder.newInstance()
                            .setSnowflakeFileTransferMetadata(metadata)
                            .setUploadStream(inputStream)
                            .setRequireCompress(true)
                            .setNetworkTimeoutInMilli(0)
                            .setOcspMode(OCSPMode.FAIL_OPEN)
                            .setSFSession(sfSession)
                            .setCommand(PUT_COMMAND)
                            .build()));
        assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), thrown.getErrorCode());
        assertTrue(
            thrown
                .getMessage()
                .contains("JDBC driver internal error: error encountered for compression"));
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testCompressStreamWithGzipNoDigestException() throws Exception {
    // inject the IOException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new IOException());

    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

        List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
        SnowflakeFileTransferMetadataV1 metadata =
            (SnowflakeFileTransferMetadataV1) metadataList.get(0);
        metadata.setEncryptionMaterial(null, null, null);

        String srcPath = getFullPathFileInResource(TEST_DATA_FILE);

        InputStream inputStream = new FileInputStream(srcPath);
        SnowflakeSQLException thrown =
            assertThrows(
                SnowflakeSQLException.class,
                () ->
                    SnowflakeFileTransferAgent.uploadWithoutConnection(
                        SnowflakeFileTransferConfig.Builder.newInstance()
                            .setSnowflakeFileTransferMetadata(metadata)
                            .setUploadStream(inputStream)
                            .setRequireCompress(true)
                            .setNetworkTimeoutInMilli(0)
                            .setOcspMode(OCSPMode.FAIL_OPEN)
                            .setSFSession(sfSession)
                            .setCommand(PUT_COMMAND)
                            .build()));
        assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), thrown.getErrorCode());
        assertTrue(
            thrown
                .getMessage()
                .contains("JDBC driver internal error: error encountered for compression"));
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testUploadWithoutConnectionException() throws Exception {
    // inject the IOException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new Exception("Exception encountered during file upload: failed to push to remote store"));

    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

        List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
        SnowflakeFileTransferMetadataV1 metadata =
            (SnowflakeFileTransferMetadataV1) metadataList.get(0);

        String srcPath = getFullPathFileInResource(TEST_DATA_FILE);

        InputStream inputStream = new FileInputStream(srcPath);
        Exception thrown =
            assertThrows(
                Exception.class,
                () ->
                    SnowflakeFileTransferAgent.uploadWithoutConnection(
                        SnowflakeFileTransferConfig.Builder.newInstance()
                            .setSnowflakeFileTransferMetadata(metadata)
                            .setUploadStream(inputStream)
                            .setRequireCompress(true)
                            .setNetworkTimeoutInMilli(0)
                            .setOcspMode(OCSPMode.FAIL_OPEN)
                            .setSFSession(sfSession)
                            .setCommand(PUT_COMMAND)
                            .build()));
        assertTrue(
            thrown
                .getMessage()
                .contains(
                    "Exception encountered during file upload: failed to push to remote store"));
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testInitFileMetadataFileNotFound() throws Exception {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

        SnowflakeSQLException thrown = assertThrows(SnowflakeSQLException.class, sfAgent::execute);
        assertEquals(ErrorCode.FILE_NOT_FOUND.getMessageCode(), thrown.getErrorCode());
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
  }

  @Test
  public void testInitFileMetadataFileIsDirectory() throws Exception {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        String srcPath =
            getFullPathFileInResource(""); // will pull the resources directory without a file
        String command = "put file://" + srcPath + " @testStage";
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
        SnowflakeSQLException thrown = assertThrows(SnowflakeSQLException.class, sfAgent::execute);
        assertEquals(ErrorCode.FILE_IS_DIRECTORY.getMessageCode(), thrown.getErrorCode());
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
  }

  @Test
  public void testCompareAndSkipFilesException() throws Exception {
    // inject the NoSuchAlgorithmException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new NoSuchAlgorithmException());

    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        String command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testStage";
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));

        SnowflakeSQLException thrown = assertThrows(SnowflakeSQLException.class, sfAgent::execute);
        assertEquals(
            (long) ErrorCode.FILE_OPERATION_UPLOAD_ERROR.getMessageCode(), thrown.getErrorCode());
        assertInstanceOf(NoSuchAlgorithmException.class, thrown.getCause().getCause());
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testParseCommandException() throws SQLException {
    // inject the SnowflakeSQLException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new SnowflakeSQLException("invalid data"));
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        SnowflakeSQLException thrown =
            assertThrows(
                SnowflakeSQLException.class,
                () ->
                    new SnowflakeFileTransferAgent(
                        PUT_COMMAND, sfSession, new SFStatement(sfSession)));
        assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), thrown.getErrorCode());
        assertTrue(thrown.getMessage().contains("Failed to parse the locations"));
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testPopulateStatusRowsWithSortOn() throws Exception {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        statement.execute("set-sf-property sort on");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

        // upload files orders_101.csv and orders_100.csv
        String command =
            "PUT file://"
                + getFullPathFileInResource("")
                + File.separator
                + "orders_10*.csv @testStage";
        SnowflakeFileTransferAgent sfAgent1 =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
        sfAgent1.execute(); // upload files

        // check that source files were sorted
        assertEquals(2, sfAgent1.statusRows.size());
        assertEquals("orders_100.csv", sfAgent1.getNextRow().get(0).toString());

        String getCommand = "GET @testStage file:///tmp";
        SnowflakeFileTransferAgent sfAgent2 =
            new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));
        sfAgent2.execute();
        // check that files are sorted on download
        assertEquals(2, sfAgent2.statusRows.size());
        assertEquals("orders_100.csv.gz", sfAgent2.getNextRow().get(0).toString());
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
  }

  @Test
  public void testListObjectsStorageException() throws Exception {
    // inject the StorageProviderException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new StorageProviderException(new Exception("could not list objects")));

    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace stage testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        String command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testStage";
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));

        SnowflakeSQLException thrown = assertThrows(SnowflakeSQLException.class, sfAgent::execute);
        assertEquals(ErrorCode.IO_ERROR.getMessageCode(), thrown.getErrorCode());
        assertTrue(thrown.getMessage().contains("Encountered exception during listObjects"));
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testUploadStreamInterruptedException() throws IOException, SQLException {
    final String DEST_PREFIX = TEST_UUID + "/testUploadStream";
    // inject the InterruptedException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(new InterruptedException());

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        FileBackedOutputStream outputStream = new FileBackedOutputStream(1000000);
        outputStream.write("hello".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        // upload the data to user stage under testUploadStream with name hello.txt
        SnowflakeSQLException thrown =
            assertThrows(
                SnowflakeSQLException.class,
                () ->
                    connection
                        .unwrap(SnowflakeConnection.class)
                        .uploadStream(
                            "~",
                            DEST_PREFIX,
                            outputStream.asByteSource().openStream(),
                            "hello.txt",
                            false));
        assertEquals(ErrorCode.INTERRUPTED.getMessageCode(), thrown.getErrorCode());
      } finally {
        statement.execute("rm @~/" + DEST_PREFIX);
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testFileTransferStageInfo() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(PUT_COMMAND, sfSession, new SFStatement(sfSession));

        StageInfo stageInfo = sfAgent.getStageInfo();
        assertEquals(sfAgent.getStageCredentials(), stageInfo.getCredentials());
        assertEquals(sfAgent.getStageLocation(), stageInfo.getLocation());
      } finally {
        statement.execute("drop stage if exists testStage");
      }
    }
  }

  @Test
  public void testFileTransferMappingFromSourceFile() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE testStage");
        SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();

        String command =
            "PUT file://"
                + getFullPathFileInResource("")
                + File.separator
                + "orders_10*.csv @testStage";
        SnowflakeFileTransferAgent sfAgent1 =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
        sfAgent1.execute();

        SnowflakeFileTransferAgent sfAgent2 =
            new SnowflakeFileTransferAgent(
                "GET @testStage file:///tmp/", sfSession, new SFStatement(sfSession));

        assertEquals(2, sfAgent2.getSrcToMaterialsMap().size());
        assertEquals(2, sfAgent2.getSrcToPresignedUrlMap().size());
      } finally {
        statement.execute("drop stage if exists testStage");
      }
    }
  }

  @Test
  public void testUploadFileCallableFileNotFound() throws Exception {
    // inject the FileNotFoundException
    SnowflakeFileTransferAgent.setInjectedFileTransferException(
        new FileNotFoundException("file does not exist"));
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE testStage");
        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        String command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testStage";
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
        Exception thrown = assertThrows(Exception.class, sfAgent::execute);
        assertInstanceOf(FileNotFoundException.class, thrown.getCause());
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
    SnowflakeFileTransferAgent.setInjectedFileTransferException(null);
  }

  @Test
  public void testUploadFileStreamWithNoOverwrite() throws Exception {
    String expectedValue = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE testStage");

        uploadFileToStageUsingStream(connection, false);
        try (ResultSet resultSet = statement.executeQuery("LIST @testStage")) {
          assertTrue(resultSet.next());
          expectedValue = resultSet.getString("last_modified");
        }
        Thread.sleep(1000); // add 1 sec delay between uploads.

        uploadFileToStageUsingStream(connection, false);
        try (ResultSet resultSet = statement.executeQuery("LIST @testStage")) {
          assertTrue(resultSet.next());
          String actualValue = resultSet.getString("last_modified");
          assertEquals(expectedValue, actualValue);
        }
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
  }

  @Test
  public void testUploadFileStreamWithOverwrite() throws Exception {
    String expectedValue = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE testStage");

        uploadFileToStageUsingStream(connection, true);
        try (ResultSet resultSet = statement.executeQuery("LIST @testStage")) {
          assertTrue(resultSet.next());
          expectedValue = resultSet.getString("last_modified");
        }
        Thread.sleep(1000); // add 1 sec delay between uploads.

        uploadFileToStageUsingStream(connection, true);
        try (ResultSet resultSet = statement.executeQuery("LIST @testStage")) {
          assertTrue(resultSet.next());
          String actualValue = resultSet.getString("last_modified");

          assertFalse(expectedValue.equals(actualValue));
        }
      } finally {
        statement.execute("DROP STAGE if exists testStage");
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetS3StorageObjectMetadata() throws Throwable {
    try (Connection connection = getConnection("s3testaccount");
        Statement statement = connection.createStatement()) {
      // create a stage to put the file in
      try {
        statement.execute("CREATE OR REPLACE STAGE " + OBJ_META_STAGE);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        // Test put file with internal compression
        String putCommand = "put file:///dummy/path/file1.gz @" + OBJ_META_STAGE;
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadata = sfAgent.getFileTransferMetadatas();

        String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
        for (SnowflakeFileTransferMetadata oneMetadata : metadata) {
          InputStream inputStream = new FileInputStream(srcPath);

          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(oneMetadata)
                  .setUploadStream(inputStream)
                  .setRequireCompress(true)
                  .setNetworkTimeoutInMilli(0)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .setSFSession(sfSession)
                  .setCommand(putCommand)
                  .build());

          SnowflakeStorageClient client =
              StorageClientFactory.getFactory()
                  .createClient(
                      ((SnowflakeFileTransferMetadataV1) oneMetadata).getStageInfo(),
                      1,
                      null,
                      /*session = */ null);

          String location =
              ((SnowflakeFileTransferMetadataV1) oneMetadata).getStageInfo().getLocation();
          int idx = location.indexOf('/');
          String remoteStageLocation = location.substring(0, idx);
          String path = location.substring(idx + 1) + "file1.gz";
          StorageObjectMetadata meta = client.getObjectMetadata(remoteStageLocation, path);

          ObjectMetadata s3Meta = new ObjectMetadata();
          s3Meta.setContentLength(meta.getContentLength());
          s3Meta.setContentEncoding(meta.getContentEncoding());
          s3Meta.setUserMetadata(meta.getUserMetadata());

          S3StorageObjectMetadata s3Metadata = new S3StorageObjectMetadata(s3Meta);
          RemoteStoreFileEncryptionMaterial encMat = sfAgent.getEncryptionMaterial().get(0);
          Map<String, String> matDesc =
              mapper.readValue(s3Metadata.getUserMetadata().get("x-amz-matdesc"), Map.class);

          assertEquals(encMat.getQueryId(), matDesc.get("queryId"));
          assertEquals(encMat.getSmkId().toString(), matDesc.get("smkId"));
          assertEquals(1360, s3Metadata.getContentLength());
          assertEquals("gzip", s3Metadata.getContentEncoding());
        }
      } finally {
        statement.execute("DROP STAGE if exists " + OBJ_META_STAGE);
      }
    }
  }

  private void uploadFileToStageUsingStream(Connection connection, boolean overwrite)
      throws Exception {
    SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);

    String putCommand = "PUT file://" + sourceFilePath + " @testStage";

    if (overwrite) {
      putCommand += " overwrite=true";
    }

    SnowflakeFileTransferAgent sfAgent =
        new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));

    InputStream is = Files.newInputStream(Paths.get(sourceFilePath));

    sfAgent.setSourceStream(is);
    sfAgent.setDestFileNameForStreamSource("test_file");

    sfAgent.execute();
  }

  @Test
  public void testUploadFileWithTildeInFolderName() throws SQLException, IOException {
    Path topDataDir = null;

    try {
      topDataDir = Files.createTempDirectory("testPutFileTilde");
      topDataDir.toFile().deleteOnExit();

      // create sub directory where the name includes ~
      Path subDir = Files.createDirectories(Paths.get(topDataDir.toString(), "snowflake~"));

      // create a test data
      File dataFile = new File(subDir.toFile(), "test.txt");
      try (Writer writer =
          new BufferedWriter(
              new OutputStreamWriter(
                  Files.newOutputStream(Paths.get(dataFile.getCanonicalPath())),
                  StandardCharsets.UTF_8))) {
        writer.write("1,test1");
      }

      try (Connection connection = getConnection();
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("create or replace stage testStage");
          String sql = String.format("PUT 'file://%s' @testStage", dataFile.getCanonicalPath());

          // Escape backslashes. This must be done by the application.
          sql = sql.replaceAll("\\\\", "\\\\\\\\");
          try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
              assertEquals("UPLOADED", resultSet.getString("status"));
            }
          }
        } finally {
          statement.execute("drop stage if exists testStage");
        }
      }
    } finally {
      FileUtils.deleteDirectory(topDataDir.toFile());
    }
  }

  @Test
  public void testUploadWithTildeInPath() throws SQLException, IOException {
    Path subDir = null;
    try {
      String homeDir = systemGetProperty("user.home");

      // create sub directory where the name includes ~
      subDir = Files.createDirectories(Paths.get(homeDir, "snowflake"));

      // create a test data
      File dataFile = new File(subDir.toFile(), "test.txt");
      try (Writer writer =
          new BufferedWriter(
              new OutputStreamWriter(
                  Files.newOutputStream(Paths.get(dataFile.getCanonicalPath())),
                  StandardCharsets.UTF_8))) {
        writer.write("1,test1");
      }
      try (Connection connection = getConnection();
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("create or replace stage testStage");

          try (ResultSet resultSet =
              statement.executeQuery("PUT 'file://~/snowflake/test.txt' @testStage")) {
            while (resultSet.next()) {
              assertEquals("UPLOADED", resultSet.getString("status"));
            }
          }
        } finally {
          statement.execute("drop stage if exists testStage");
        }
      }
    } finally {
      FileUtils.deleteDirectory(subDir.toFile());
    }
  }

  @Test
  public void testUploadWithTripleSlashFilePrefix(@TempDir File tempDir)
      throws SQLException, IOException {
    String stageName = "testStage" + SnowflakeUtil.randomAlphaNumeric(10);
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE STAGE " + stageName);
        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        String command =
            "PUT file:///" + getFullPathFileInResource(TEST_DATA_FILE) + " @" + stageName;
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(command, sfSession, new SFStatement(sfSession));
        assertTrue(sfAgent.execute());

        String tempDirPath = tempDir.getCanonicalPath().replace("\\", "/");
        String getCommand = "GET @" + stageName + " file:///" + tempDirPath;
        SnowflakeFileTransferAgent sfAgent1 =
            new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));
        assertTrue(sfAgent1.execute());
        assertEquals(1, sfAgent1.statusRows.size());
      } finally {
        statement.execute("DROP STAGE if exists " + stageName);
      }
    }
  }
}
