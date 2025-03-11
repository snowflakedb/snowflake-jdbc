package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeDriver.getClientVersionStringFromManifest;
import static net.snowflake.client.jdbc.SnowflakeDriver.implementVersion;
import static net.snowflake.client.jdbc.SnowflakeDriverIT.findFile;
import static net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1.mapper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.storage.StorageException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.annotations.DontRunOnTestaccount;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeGCSClient;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeStorageClient;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.cloud.storage.StorageClientFactory;
import net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * General JDBC tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to SnowflakeDriverIT so that both the latest
 * and oldest supported driver run the tests.
 */
@Tag(TestTags.OTHERS)
public class SnowflakeDriverLatestIT extends BaseJDBCTest {
  @TempDir private File tmpFolder;
  @TempDir private File tmpFolder2;

  public String testStageName =
      String.format("test_stage_%s", UUID.randomUUID().toString()).replaceAll("-", "_");

  // Check whether the two file's content are logically identical
  private boolean isFileContentEqual(
      String fileFullPath1, boolean compressedFile1, String fileFullPath2, boolean compressedFile2)
      throws Throwable {
    InputStream inputStream1 = new FileInputStream(fileFullPath1);
    InputStream inputStream2 = new FileInputStream(fileFullPath2);

    try {
      if (compressedFile1) {
        inputStream1 = new GZIPInputStream(inputStream1);
      }
      if (compressedFile2) {
        inputStream2 = new GZIPInputStream(inputStream2);
      }
      return Arrays.equals(IOUtils.toByteArray(inputStream1), IOUtils.toByteArray(inputStream2));
    } finally {
      inputStream1.close();
      inputStream2.close();
    }
  }

  @Test
  public void testStaticVersionMatchesManifest() {
    assertEquals(implementVersion, getClientVersionStringFromManifest().replace("-SNAPSHOT", ""));
  }

  @Test
  @DontRunOnTestaccount
  public void testClientInfoConnectionProperty() throws Throwable {
    String clientInfoJSONStr = null;
    JsonNode clientInfoJSON = null;
    Properties props = new Properties();
    props.put(
        "snowflakeClientInfo",
        "{\"spark.version\":\"3.0.0\", \"spark.snowflakedb.version\":\"2.8.5\","
            + " \"spark.app.name\":\"SnowflakeSourceSuite\", \"scala.version\":\"2.12.11\","
            + " \"java.version\":\"1.8.0_221\", \"snowflakedb.jdbc.version\":\"3.13.2\"}");
    try (Connection connection = getConnection(DONT_INJECT_SOCKET_TIMEOUT, props, false, false);
        Statement statement = connection.createStatement();
        ResultSet res = statement.executeQuery("select current_session_client_info()")) {
      assertTrue(res.next());
      clientInfoJSONStr = res.getString(1);
      clientInfoJSON = mapper.readTree(clientInfoJSONStr);
      // assert that spart version and spark app are found
      assertEquals("3.0.0", clientInfoJSON.get("spark.version").asText(), "spark version mismatch");
      assertEquals(
          "SnowflakeSourceSuite",
          clientInfoJSON.get("spark.app.name").asText(),
          "spark app mismatch");
    }

    // Test that when session property is set, connection parameter overrides it
    System.setProperty(
        "snowflake.client.info",
        "{\"spark.version\":\"fake\", \"spark.snowflakedb.version\":\"fake\","
            + " \"spark.app.name\":\"fake\", \"scala.version\":\"fake\","
            + " \"java.version\":\"fake\", \"snowflakedb.jdbc.version\":\"fake\"}");
    try (Connection connection = getConnection(DONT_INJECT_SOCKET_TIMEOUT, props, false, false);
        Statement statement = connection.createStatement();
        ResultSet res = statement.executeQuery("select current_session_client_info()")) {
      assertTrue(res.next());
      clientInfoJSONStr = res.getString(1);
      clientInfoJSON = mapper.readTree(clientInfoJSONStr);
      // assert that spart version and spark app are found
      assertEquals("3.0.0", clientInfoJSON.get("spark.version").asText(), "spark version mismatch");
      assertEquals(
          "SnowflakeSourceSuite",
          clientInfoJSON.get("spark.app.name").asText(),
          "spark app mismatch");
    }
    System.clearProperty("snowflake.client.info");
  }

  @Test
  public void testGetSessionID() throws Throwable {
    try (Connection con = getConnection();
        Statement statement = con.createStatement();
        ResultSet rset = statement.executeQuery("select current_session()")) {
      String sessionID = con.unwrap(SnowflakeConnection.class).getSessionID();
      assertTrue(rset.next());
      assertEquals(sessionID, rset.getString(1));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutThreshold() throws SQLException {
    try (Connection connection = getConnection()) {
      // assert that threshold equals default 200 from server side
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      try (Statement statement = connection.createStatement()) {
        SFStatement sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
        statement.execute("CREATE OR REPLACE STAGE PUTTHRESHOLDSTAGE");
        String command =
            "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @PUTTHRESHOLDSTAGE";
        SnowflakeFileTransferAgent agent =
            new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
        assertEquals(200 * 1024 * 1024, agent.getBigFileThreshold());
        // assert that setting threshold via put statement directly sets the big file threshold
        // appropriately
        String commandWithPut = command + " threshold=314572800";
        agent = new SnowflakeFileTransferAgent(commandWithPut, sfSession, sfStatement);
        assertEquals(314572800, agent.getBigFileThreshold());
        // assert that after put statement, threshold goes back to previous session threshold
        agent = new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
        assertEquals(200 * 1024 * 1024, agent.getBigFileThreshold());
        // Attempt to set threshold to an invalid value such as a negative number
        String commandWithInvalidThreshold = command + " threshold=-1";
        SQLException e =
            assertThrows(
                SQLException.class,
                () ->
                    new SnowflakeFileTransferAgent(
                        commandWithInvalidThreshold, sfSession, sfStatement));
        assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
      }
    } catch (SQLException ex) {
      throw ex;
    }
  }

  /** Test API for Spark connector for FileTransferMetadata */
  @Test
  @Disabled
  public void testGCPFileTransferMetadataWithOneFile() throws Throwable {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();

    try (Connection connection = getConnection("gcpaccount");
        Statement statement = connection.createStatement()) {
      try {
        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE " + testStageName);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        // Test put file with internal compression
        String putCommand1 = "put file:///dummy/path/file1.gz @" + testStageName;
        SnowflakeFileTransferAgent sfAgent1 =
            new SnowflakeFileTransferAgent(putCommand1, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadatas1 = sfAgent1.getFileTransferMetadatas();

        String srcPath1 = getFullPathFileInResource(TEST_DATA_FILE);
        for (SnowflakeFileTransferMetadata oneMetadata : metadatas1) {
          InputStream inputStream = new FileInputStream(srcPath1);

          assertTrue(oneMetadata.isForOneFile());
          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(oneMetadata)
                  .setUploadStream(inputStream)
                  .setRequireCompress(true)
                  .setNetworkTimeoutInMilli(0)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .build());
        }

        // Test Put file with external compression
        String putCommand2 = "put file:///dummy/path/file2.gz @" + testStageName;
        SnowflakeFileTransferAgent sfAgent2 =
            new SnowflakeFileTransferAgent(putCommand2, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadatas2 = sfAgent2.getFileTransferMetadatas();

        String srcPath2 = getFullPathFileInResource(TEST_DATA_FILE_2);
        for (SnowflakeFileTransferMetadata oneMetadata : metadatas2) {
          String gzfilePath = destFolderCanonicalPath + "/tmp_compress.gz";
          Process p =
              Runtime.getRuntime()
                  .exec("cp -fr " + srcPath2 + " " + destFolderCanonicalPath + "/tmp_compress");
          p.waitFor();
          p = Runtime.getRuntime().exec("gzip " + destFolderCanonicalPath + "/tmp_compress");
          p.waitFor();

          InputStream gzInputStream = new FileInputStream(gzfilePath);
          assertTrue(oneMetadata.isForOneFile());
          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(oneMetadata)
                  .setUploadStream(gzInputStream)
                  .setRequireCompress(false)
                  .setNetworkTimeoutInMilli(0)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .build());
        }

        // Download two files and verify their content.
        assertTrue(
            statement.execute(
                "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/' parallel=8"),
            "Failed to get files");

        // Make sure that the downloaded files are EQUAL,
        // they should be gzip compressed
        assertTrue(
            isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
        assertTrue(
            isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
      } finally {
        statement.execute("DROP STAGE if exists " + testStageName);
      }
    }
  }

  /** Test API for Kafka connector for FileTransferMetadata */
  @Test
  @DontRunOnGithubActions
  public void testAzureS3FileTransferMetadataWithOneFile() throws Throwable {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();

    List<String> supportedAccounts = Arrays.asList("s3testaccount", "azureaccount");
    for (String accountName : supportedAccounts) {
      try (Connection connection = getConnection(accountName);
          Statement statement = connection.createStatement()) {
        try {
          // create a stage to put the file in
          statement.execute("CREATE OR REPLACE STAGE " + testStageName);

          SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

          // Test put file with internal compression
          String putCommand1 = "put file:///dummy/path/file1.gz @" + testStageName;
          SnowflakeFileTransferAgent sfAgent1 =
              new SnowflakeFileTransferAgent(putCommand1, sfSession, new SFStatement(sfSession));
          List<SnowflakeFileTransferMetadata> metadatas1 = sfAgent1.getFileTransferMetadatas();

          String srcPath1 = getFullPathFileInResource(TEST_DATA_FILE);
          for (SnowflakeFileTransferMetadata oneMetadata : metadatas1) {
            InputStream inputStream = new FileInputStream(srcPath1);

            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(inputStream)
                    .setRequireCompress(true)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .setSFSession(sfSession)
                    .setCommand(putCommand1)
                    .build());
          }

          // Test Put file with external compression
          String putCommand2 = "put file:///dummy/path/file2.gz @" + testStageName;
          SnowflakeFileTransferAgent sfAgent2 =
              new SnowflakeFileTransferAgent(putCommand2, sfSession, new SFStatement(sfSession));
          List<SnowflakeFileTransferMetadata> metadatas2 = sfAgent2.getFileTransferMetadatas();

          String srcPath2 = getFullPathFileInResource(TEST_DATA_FILE_2);
          for (SnowflakeFileTransferMetadata oneMetadata : metadatas2) {
            String gzfilePath = destFolderCanonicalPath + "/tmp_compress.gz";
            Process p =
                Runtime.getRuntime()
                    .exec("cp -fr " + srcPath2 + " " + destFolderCanonicalPath + "/tmp_compress");
            p.waitFor();
            p = Runtime.getRuntime().exec("gzip " + destFolderCanonicalPath + "/tmp_compress");
            p.waitFor();

            InputStream gzInputStream = new FileInputStream(gzfilePath);

            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(gzInputStream)
                    .setRequireCompress(false)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .setSFSession(sfSession)
                    .setCommand(putCommand2)
                    .build());
          }

          // Download two files and verify their content.
          assertTrue(
              statement.execute(
                  "GET @"
                      + testStageName
                      + " 'file://"
                      + destFolderCanonicalPath
                      + "/' parallel=8"),
              "Failed to get files");

          // Make sure that the downloaded files are EQUAL,
          // they should be gzip compressed
          assertTrue(
              isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
          assertTrue(
              isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
        } finally {
          statement.execute("DROP STAGE if exists " + testStageName);
        }
      }
    }
  }

  /** Negative test for FileTransferMetadata. It is only supported for PUT. */
  @Test
  @DontRunOnGithubActions
  public void testGCPFileTransferMetadataNegativeOnlySupportPut() throws Throwable {
    try (Connection connection = getConnection("gcpaccount");
        Statement statement = connection.createStatement()) {
      try {
        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE " + testStageName);

        // Put one file to the stage
        String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
        statement.execute("put file://" + srcPath + " @" + testStageName);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        File destFolder = new File(tmpFolder, "dest");
        destFolder.mkdirs();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();

        String getCommand = "get @" + testStageName + " file://" + destFolderCanonicalPath;

        // The GET can be executed in normal way.
        statement.execute(getCommand);

        // Start negative test for GET.
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));

        assertThrows(Exception.class, sfAgent::getFileTransferMetadatas);
      } finally {
        statement.execute("DROP STAGE if exists " + testStageName);
      }
    }
  }

  @Test
  public void testGetPropertyInfo() throws SQLException {
    // Test with blank URL and no properties. ServerURL is needed.
    String url = "";
    Properties props = new Properties();
    Driver driver = DriverManager.getDriver("jdbc:snowflake://snowflake.reg.local:8082");
    DriverPropertyInfo[] info = driver.getPropertyInfo(url, props);
    assertEquals(1, info.length);
    assertEquals("serverURL", info[0].name);
    assertEquals(
        "server URL in form of <protocol>://<host or domain>:<port number>/<path of resource>",
        info[0].description);

    // Test with null URL and no properties. ServerURL is needed.
    url = null;
    props = new Properties();
    driver = DriverManager.getDriver("jdbc:snowflake://snowflake.reg.local:8082");
    info = driver.getPropertyInfo(url, props);
    assertEquals(1, info.length);
    assertEquals("serverURL", info[0].name);
    assertEquals(
        "server URL in form of <protocol>://<host or domain>:<port number>/<path of resource>",
        info[0].description);

    // Test with URL that requires username and password.
    url = "jdbc:snowflake://snowflake.reg.local:8082";
    info = driver.getPropertyInfo(url, props);
    assertEquals(2, info.length);
    assertEquals("user", info[0].name);
    assertEquals("username for account", info[0].description);
    assertEquals("password", info[1].name);
    assertEquals("password for account", info[1].description);

    // Add username and try again; get password requirement back
    props.put("user", "snowman");
    props.put("password", "test");
    info = driver.getPropertyInfo(url, props);
    assertEquals(0, info.length);

    props.put("useProxy", "true");
    info = driver.getPropertyInfo(url, props);
    assertEquals(2, info.length);
    assertEquals("proxyHost", info[0].name);
    assertEquals("proxy host name", info[0].description);
    assertEquals("proxyPort", info[1].name);
    assertEquals("proxy port; should be an integer", info[1].description);

    props.put("proxyHost", "dummyHost");
    props.put("proxyPort", "dummyPort");
    info = driver.getPropertyInfo(url, props);
    assertEquals(0, info.length);

    // invalid URL still throws SQLException
    String invalidUrl = "snowflake.reg.local:8082";
    Properties fileProps = new Properties();
    Driver finalDriver = driver;
    SQLException e =
        assertThrows(SQLException.class, () -> finalDriver.getPropertyInfo(invalidUrl, fileProps));
    assertEquals((int) ErrorCode.INVALID_CONNECT_STRING.getMessageCode(), e.getErrorCode());
  }

  /**
   * Tests put with overwrite set to false. Require SNOW-206907 to be merged to pass tests for gcp
   * account
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testPutOverwriteFalseNoDigest() throws Throwable {

    // create 2 files: an original, and one that will overwrite the original
    File file1 = new File(tmpFolder, "testfile.csv");
    file1.createNewFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file1))) {
      bw.write("Writing original file content. This should get overwritten.");
    }

    File file2 = new File(tmpFolder2, "testfile.csv");
    file2.createNewFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file2))) {
      bw.write("This is all new! This should be the result of the overwriting.");
    }
    String sourceFilePathOriginal = file1.getCanonicalPath();
    String sourceFilePathOverwrite = file2.getCanonicalPath();

    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i), paramProperties);
          Statement statement = connection.createStatement()) {
        try {
          // create a stage to put the file in
          statement.execute("CREATE OR REPLACE STAGE testing_stage");
          assertTrue(
              statement.execute("PUT file://" + sourceFilePathOriginal + " @testing_stage"),
              "Failed to put a file");
          // check that file exists in stage after PUT
          findFile(statement, "ls @testing_stage/");

          // put another file in same stage with same filename with overwrite = true
          assertTrue(
              statement.execute(
                  "PUT file://" + sourceFilePathOverwrite + " @testing_stage overwrite=false"),
              "Failed to put a file");

          // check that file exists in stage after PUT
          findFile(statement, "ls @testing_stage/");

          // get file from new stage
          assertTrue(
              statement.execute(
                  "GET @testing_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
              "Failed to get files");

          // Make sure that the downloaded file exists; it should be gzip compressed
          File downloaded = new File(destFolderCanonicalPathWithSeparator + "testfile.csv.gz");
          assertTrue(downloaded.exists());

          // unzip the file
          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "testfile.csv.gz");
          p.waitFor();

          // 2nd file should never be uploaded
          File unzipped = new File(destFolderCanonicalPathWithSeparator + "testfile.csv");
          assertTrue(FileUtils.contentEqualsIgnoreEOL(file1, unzipped, null));
        } finally {
          statement.execute("DROP TABLE IF EXISTS testLoadToLocalFS");
        }
      }
    }
  }

  /**
   * Tests PUT disable test
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testPutDisable() throws Throwable {

    // create a file
    File file = new File(tmpFolder, "testfile99.csv");
    file.createNewFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
      bw.write("This content won't be uploaded as PUT is disabled.");
    }

    String sourceFilePathOriginal = file.getCanonicalPath();

    Properties paramProperties = new Properties();
    paramProperties.put("enablePutGet", false);

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i), paramProperties);
          Statement statement = connection.createStatement()) {
        Exception ex =
            assertThrows(
                Exception.class,
                () ->
                    statement.execute(
                        "PUT file://" + sourceFilePathOriginal + " @testPutGet_disable_stage"));
        assertTrue(ex.getMessage().equalsIgnoreCase("File transfers have been disabled."));
      }
    }
  }

  /**
   * Tests GET disable test
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testGetDisable() throws Throwable {

    // create a folder
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();

    Properties paramProperties = new Properties();
    paramProperties.put("enablePutGet", false);

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i), paramProperties);
          Statement statement = connection.createStatement()) {

        Exception ex =
            assertThrows(
                Exception.class,
                () ->
                    statement.execute(
                        "GET @testPutGet_disable_stage 'file://"
                            + destFolderCanonicalPath
                            + "' parallel=8"));

        assertTrue(ex.getMessage().equalsIgnoreCase("File transfers have been disabled."));
      }
    }
  }

  /**
   * Test NULL in LIMIT and OFFSET with Snow-76376 enabled this should be handled as without LIMIT
   * and OFFSET
   */
  @Test
  public void testSnow76376() throws Throwable {
    try (Connection connection = getConnection();
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute(
            "create or replace table t(a int) as select * from values" + "(1),(2),(8),(10)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * FROM t " + "ORDER BY a LIMIT " + "? OFFSET ?")) {

          ////////////////////////////
          // both NULL
          preparedStatement.setNull(1, 4); // int
          preparedStatement.setNull(2, 4); // int

          assertTrue(
              preparedStatement.execute(),
              "Could not execute preparedStatement with OFFSET and LIMIT set to NULL");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(8, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(10, resultSet.getInt(1));
          }

          ////////////////////////////
          // both empty string
          preparedStatement.setString(1, "");
          preparedStatement.setString(2, "");

          assertTrue(
              preparedStatement.execute(),
              "Could not execute preparedStatement with OFFSET and LIMIT set to empty string");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(8, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(10, resultSet.getInt(1));
          }

          ////////////////////////////
          // only LIMIT NULL
          preparedStatement.setNull(1, 4); // int
          preparedStatement.setInt(2, 2);

          assertTrue(
              preparedStatement.execute(),
              "Could not execute preparedStatement with LIMIT set to NULL");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(8, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(10, resultSet.getInt(1));
          }

          ////////////////////////////
          // only LIMIT empty string
          preparedStatement.setString(1, "");
          preparedStatement.setInt(2, 2);

          assertTrue(
              preparedStatement.execute(),
              "Could not execute preparedStatement with LIMIT set to empty string");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(8, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(10, resultSet.getInt(1));
          }

          ////////////////////////////
          // only OFFSET NULL
          preparedStatement.setInt(1, 3); // int
          preparedStatement.setNull(2, 4);

          assertTrue(
              preparedStatement.execute(),
              "Could not execute preparedStatement with OFFSET set to NULL");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(8, resultSet.getInt(1));
          }

          ////////////////////////////
          // only OFFSET empty string
          preparedStatement.setInt(1, 3); // int
          preparedStatement.setNull(2, 4);

          assertTrue(
              preparedStatement.execute(),
              "Could not execute preparedStatement with OFFSET set to empty string");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(2, resultSet.getInt(1));
            assertTrue(resultSet.next());
            assertEquals(8, resultSet.getInt(1));
          }
        }
        ////////////////////////////
        // OFFSET and LIMIT NULL for constant select query
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT 1 FROM t " + "ORDER BY a LIMIT " + "? OFFSET ?")) {
          preparedStatement.setNull(1, 4); // int
          preparedStatement.setNull(2, 4); // int

          assertTrue(
              preparedStatement.execute(),
              "Could not execute constant preparedStatement with OFFSET and LIMIT set to NULL");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            for (int i = 0; i < 4; i++) {
              assertTrue(resultSet.next());
              assertEquals(1, resultSet.getInt(1));
            }
          }

          ////////////////////////////
          // OFFSET and LIMIT empty string for constant select query
          preparedStatement.setString(1, ""); // int
          preparedStatement.setString(2, ""); // int

          assertTrue(
              preparedStatement.execute(),
              "Could not execute constant preparedStatement with OFFSET and LIMIT set to empty string");
          try (ResultSet resultSet = preparedStatement.getResultSet()) {
            for (int i = 0; i < 4; i++) {
              assertTrue(resultSet.next());
              assertEquals(1, resultSet.getInt(1));
            }
          }
        }
      } finally {
        regularStatement.execute("drop table t");
      }
    }
  }

  /**
   * Tests that result columns of type GEOGRAPHY appear as VARCHAR / VARIANT / BINARY to the client,
   * depending on the value of GEOGRAPHY_OUTPUT_FORMAT
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testGeoOutputTypes() throws Throwable {

    Properties paramProperties = new Properties();

    paramProperties.put("ENABLE_USER_DEFINED_TYPE_EXPANSION", true);
    paramProperties.put("ENABLE_GEOGRAPHY_TYPE", true);

    try (Connection connection = getConnection(paramProperties);
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute("create or replace table t_geo(geo geography);");

        regularStatement.execute(
            "insert into t_geo values ('POINT(0 0)'), ('LINESTRING(1 1, 2 2)')");

        testGeoOutputTypeSingle(
            regularStatement, false, "geoJson", "OBJECT", "java.lang.String", Types.VARCHAR);

        testGeoOutputTypeSingle(
            regularStatement, true, "geoJson", "GEOGRAPHY", "java.lang.String", Types.VARCHAR);

        testGeoOutputTypeSingle(
            regularStatement, false, "wkt", "VARCHAR", "java.lang.String", Types.VARCHAR);

        testGeoOutputTypeSingle(
            regularStatement, true, "wkt", "GEOGRAPHY", "java.lang.String", Types.VARCHAR);

        testGeoOutputTypeSingle(regularStatement, false, "wkb", "BINARY", "[B", Types.BINARY);

        testGeoOutputTypeSingle(regularStatement, true, "wkb", "GEOGRAPHY", "[B", Types.BINARY);
      } finally {
        regularStatement.execute("drop table t_geo");
      }
    }
  }

  private void testGeoOutputTypeSingle(
      Statement regularStatement,
      boolean enableExternalTypeNames,
      String outputFormat,
      String expectedColumnTypeName,
      String expectedColumnClassName,
      int expectedColumnType)
      throws Throwable {

    regularStatement.execute("alter session set GEOGRAPHY_OUTPUT_FORMAT='" + outputFormat + "'");

    regularStatement.execute(
        "alter session set ENABLE_UDT_EXTERNAL_TYPE_NAMES=" + enableExternalTypeNames);

    try (ResultSet resultSet = regularStatement.executeQuery("select * from t_geo")) {
      ResultSetMetaData metadata = resultSet.getMetaData();

      assertEquals(1, metadata.getColumnCount());

      // GeoJSON: SQL type OBJECT, Java type String
      assertEquals(expectedColumnTypeName, metadata.getColumnTypeName(1));
      assertEquals(expectedColumnClassName, metadata.getColumnClassName(1));
      assertEquals(expectedColumnType, metadata.getColumnType(1));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGeoMetadata() throws Throwable {
    Properties paramProperties = new Properties();

    paramProperties.put("ENABLE_FIX_182763", true);

    try (Connection connection = getConnection(paramProperties);
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute("create or replace table t_geo(geo geography);");

        testGeoMetadataSingle(connection, regularStatement, "geoJson", Types.VARCHAR);

        testGeoMetadataSingle(connection, regularStatement, "geoJson", Types.VARCHAR);

        testGeoMetadataSingle(connection, regularStatement, "wkt", Types.VARCHAR);

        testGeoMetadataSingle(connection, regularStatement, "wkt", Types.VARCHAR);

        testGeoMetadataSingle(connection, regularStatement, "wkb", Types.BINARY);

        testGeoMetadataSingle(connection, regularStatement, "wkb", Types.BINARY);
      } finally {
        regularStatement.execute("drop table t_geo");
      }
    }
  }

  private void testGeoMetadataSingle(
      Connection connection,
      Statement regularStatement,
      String outputFormat,
      int expectedColumnType)
      throws Throwable {

    regularStatement.execute("alter session set GEOGRAPHY_OUTPUT_FORMAT='" + outputFormat + "'");

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet resultSet = md.getColumns(null, null, "T_GEO", null)) {
      ResultSetMetaData metadata = resultSet.getMetaData();

      assertEquals(24, metadata.getColumnCount());

      assertTrue(resultSet.next());

      assertEquals(expectedColumnType, resultSet.getInt(5));
      assertEquals("GEOGRAPHY", resultSet.getString(6));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGeometryOutputTypes() throws Throwable {
    Properties paramProperties = new Properties();

    paramProperties.put("ENABLE_USER_DEFINED_TYPE_EXPANSION", true);
    paramProperties.put("ENABLE_GEOMETRY_TYPE", true);

    try (Connection connection = getConnection(paramProperties);
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute("create or replace table t_geo2(geo geometry);");

        regularStatement.execute(
            "insert into t_geo2 values ('POINT(0 0)'), ('LINESTRING(1 1, 2 2)')");

        testGeometryOutputTypeSingle(
            regularStatement, true, "geoJson", "GEOMETRY", "java.lang.String", Types.VARCHAR);

        testGeometryOutputTypeSingle(
            regularStatement, true, "wkt", "GEOMETRY", "java.lang.String", Types.VARCHAR);
      } finally {
        regularStatement.execute("drop table t_geo2");
      }
    }
  }

  private void testGeometryOutputTypeSingle(
      Statement regularStatement,
      boolean enableExternalTypeNames,
      String outputFormat,
      String expectedColumnTypeName,
      String expectedColumnClassName,
      int expectedColumnType)
      throws Throwable {

    regularStatement.execute("alter session set GEOGRAPHY_OUTPUT_FORMAT='" + outputFormat + "'");

    regularStatement.execute(
        "alter session set ENABLE_UDT_EXTERNAL_TYPE_NAMES=" + enableExternalTypeNames);

    try (ResultSet resultSet = regularStatement.executeQuery("select * from t_geo2")) {

      ResultSetMetaData metadata = resultSet.getMetaData();

      assertEquals(1, metadata.getColumnCount());

      // GeoJSON: SQL type OBJECT, Java type String
      assertEquals(expectedColumnTypeName, metadata.getColumnTypeName(1));
      assertEquals(expectedColumnClassName, metadata.getColumnClassName(1));
      assertEquals(expectedColumnType, metadata.getColumnType(1));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGeometryMetadata() throws Throwable {

    Properties paramProperties = new Properties();

    try (Connection connection = getConnection(paramProperties);
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute("create or replace table t_geo2(geo geometry);");

        testGeometryMetadataSingle(connection, regularStatement, "geoJson", Types.VARCHAR);

        testGeometryMetadataSingle(connection, regularStatement, "wkt", Types.VARCHAR);
      } finally {
        regularStatement.execute("drop table t_geo2");
      }
    }
  }

  private void testGeometryMetadataSingle(
      Connection connection,
      Statement regularStatement,
      String outputFormat,
      int expectedColumnType)
      throws Throwable {

    regularStatement.execute("alter session set GEOGRAPHY_OUTPUT_FORMAT='" + outputFormat + "'");

    DatabaseMetaData md = connection.getMetaData();
    try (ResultSet resultSet = md.getColumns(null, null, "T_GEO2", null)) {
      ResultSetMetaData metadata = resultSet.getMetaData();

      assertEquals(24, metadata.getColumnCount());

      assertTrue(resultSet.next());

      assertEquals(expectedColumnType, resultSet.getInt(5));
      assertEquals("GEOMETRY", resultSet.getString(6));
    }
  }

  /**
   * Tests that upload and download small file to gcs stage. Require SNOW-206907 to be merged (still
   * pass when not merge, but will use presigned url instead of GoogleCredential)
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testPutGetGcsDownscopedCredential() throws Throwable {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      putAndGetFile(statement);
    }
  }

  /** Added in > 3.15.0 */
  @Test
  @DontRunOnGithubActions
  public void testPutGetGcsDownscopedCredentialWithDisabledDefaultCredentials() throws Throwable {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    paramProperties.put(SFSessionProperty.DISABLE_GCS_DEFAULT_CREDENTIALS.getPropertyKey(), true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      putAndGetFile(statement);
    }
  }

  private void putAndGetFile(Statement statement) throws Throwable {
    String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE_2);

    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    try {
      statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");

      assertTrue(
          statement.execute("PUT file://" + sourceFilePath + " @testPutGet_stage"),
          "Failed to put a file");

      findFile(statement, "ls @testPutGet_stage/");

      // download the file we just uploaded to stage
      assertTrue(
          statement.execute(
              "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
          "Failed to get a file");

      // Make sure that the downloaded file exists, it should be gzip compressed
      File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE_2 + ".gz");
      assertTrue(downloaded.exists());

      Process p =
          Runtime.getRuntime()
              .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE_2 + ".gz");
      p.waitFor();

      File original = new File(sourceFilePath);
      File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE_2);
      System.out.println(
          "Original file: " + original.getAbsolutePath() + ", size: " + original.length());
      System.out.println(
          "Unzipped file: " + unzipped.getAbsolutePath() + ", size: " + unzipped.length());
      assertEquals(original.length(), unzipped.length());
    } finally {
      statement.execute("DROP STAGE IF EXISTS testGetPut_stage");
    }
  }

  /**
   * Tests that upload and download big file to gcs stage. Require SNOW-206907 to be merged (still
   * pass when not merge, but will use presigned url instead of GoogleCredential)
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testPutGetLargeFileGCSDownscopedCredential() throws Throwable {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        File destFolder = new File(tmpFolder, "dest");
        destFolder.mkdirs();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

        File largeTempFile = new File(tmpFolder, "largeFile.csv");
        largeTempFile.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(largeTempFile))) {
          bw.write("Creating large test file for GCP PUT/GET test");
          bw.write(System.lineSeparator());
          bw.write("Creating large test file for GCP PUT/GET test");
          bw.write(System.lineSeparator());
        }
        File largeTempFile2 = new File(tmpFolder, "largeFile2.csv");
        largeTempFile2.createNewFile();

        String sourceFilePath = largeTempFile.getCanonicalPath();

        // copy info from 1 file to another and continue doubling file size until we reach ~1.5GB,
        // which is a large file
        for (int i = 0; i < 12; i++) {
          copyContentFrom(largeTempFile, largeTempFile2);
          copyContentFrom(largeTempFile2, largeTempFile);
        }

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE largefile_stage");
        assertTrue(
            statement.execute("PUT file://" + sourceFilePath + " @largefile_stage"),
            "Failed to put a file");

        // check that file exists in stage after PUT
        findFile(statement, "ls @largefile_stage/");

        // create a new table with columns matching CSV file
        statement.execute("create or replace table large_table (colA string)");
        // copy rows from file into table
        statement.execute("copy into large_table from @largefile_stage/largeFile.csv.gz");
        // copy back from table into different stage
        statement.execute("create or replace stage extra_stage");
        statement.execute("copy into @extra_stage/bigFile.csv.gz from large_table single=true");

        // get file from new stage
        assertTrue(
            statement.execute(
                "GET @extra_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
            "Failed to get files");

        // Make sure that the downloaded file exists; it should be gzip compressed
        File downloaded = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
        assertTrue(downloaded.exists());

        // unzip the file
        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
        p.waitFor();

        // compare the original file with the file that's been uploaded, copied into a table, copied
        // back into a stage,
        // downloaded, and unzipped
        File unzipped = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv");
        assertEquals(largeTempFile.length(), unzipped.length());
        assertTrue(FileUtils.contentEquals(largeTempFile, unzipped));
      } finally {
        statement.execute("DROP STAGE IF EXISTS largefile_stage");
        statement.execute("DROP STAGE IF EXISTS extra_stage");
        statement.execute("DROP TABLE IF EXISTS large_table");
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutGetLargeFileAzure() throws Throwable {
    Properties paramProperties = new Properties();
    try (Connection connection = getConnection("azureaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        File destFolder = new File(tmpFolder, "dest");
        destFolder.mkdirs();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

        File largeTempFile = new File(tmpFolder, "largeFile.csv");
        largeTempFile.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(largeTempFile))) {
          bw.write("Creating large test file for Azure PUT/GET test");
          bw.write(System.lineSeparator());
          bw.write("Creating large test file for Azure PUT/GET test");
          bw.write(System.lineSeparator());
        }
        File largeTempFile2 = new File(tmpFolder, "largeFile2.csv");
        largeTempFile2.createNewFile();

        String sourceFilePath = largeTempFile.getCanonicalPath();

        // copy info from 1 file to another and continue doubling file size until we reach ~1.5GB,
        // which is a large file
        for (int i = 0; i < 12; i++) {
          copyContentFrom(largeTempFile, largeTempFile2);
          copyContentFrom(largeTempFile2, largeTempFile);
        }

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE largefile_stage");
        assertTrue(
            statement.execute("PUT file://" + sourceFilePath + " @largefile_stage"),
            "Failed to put a file");

        // check that file exists in stage after PUT
        findFile(statement, "ls @largefile_stage/");

        // create a new table with columns matching CSV file
        statement.execute("create or replace table large_table (colA string)");
        // copy rows from file into table
        statement.execute("copy into large_table from @largefile_stage/largeFile.csv.gz");
        // copy back from table into different stage
        statement.execute("create or replace stage extra_stage");
        statement.execute("copy into @extra_stage/bigFile.csv.gz from large_table single=true");

        // get file from new stage
        assertTrue(
            statement.execute(
                "GET @extra_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
            "Failed to get files");

        // Make sure that the downloaded file exists; it should be gzip compressed
        File downloaded = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
        assertTrue(downloaded.exists());

        // unzip the file
        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
        p.waitFor();

        // compare the original file with the file that's been uploaded, copied into a table, copied
        // back into a stage,
        // downloaded, and unzipped
        File unzipped = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv");
        assertEquals(largeTempFile.length(), unzipped.length());
        assertTrue(FileUtils.contentEquals(largeTempFile, unzipped));
      } finally {
        statement.execute("DROP STAGE IF EXISTS largefile_stage");
        statement.execute("DROP STAGE IF EXISTS extra_stage");
        statement.execute("DROP TABLE IF EXISTS large_table");
      }
    }
  }

  /**
   * helper function for creating large file in Java. Copies info from 1 file to another
   *
   * @param file1 file with info to be copied
   * @param file2 file to be copied into
   * @throws Exception
   */
  private void copyContentFrom(File file1, File file2) throws Exception {
    FileInputStream inputStream = new FileInputStream(file1);
    FileOutputStream outputStream = new FileOutputStream(file2);
    try (FileChannel fIn = inputStream.getChannel();
        FileChannel fOut = outputStream.getChannel()) {
      fOut.transferFrom(fIn, 0, fIn.size());
      fIn.position(0);
      fOut.transferFrom(fIn, fIn.size(), fIn.size());
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutS3RegionalUrl() throws Throwable {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();

    List<String> supportedAccounts = Arrays.asList("s3testaccount", "azureaccount");
    for (String accountName : supportedAccounts) {
      try (Connection connection = getConnection(accountName);
          Statement statement = connection.createStatement()) {
        try {
          // create a stage to put the file in
          statement.execute("CREATE OR REPLACE STAGE " + testStageName);

          SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

          // Test put file with internal compression
          String putCommand1 = "put file:///dummy/path/file1.gz @" + testStageName;
          SnowflakeFileTransferAgent sfAgent1 =
              new SnowflakeFileTransferAgent(putCommand1, sfSession, new SFStatement(sfSession));
          List<SnowflakeFileTransferMetadata> metadatas1 = sfAgent1.getFileTransferMetadatas();

          String srcPath1 = getFullPathFileInResource(TEST_DATA_FILE);

          for (SnowflakeFileTransferMetadata oneMetadata : metadatas1) {
            InputStream inputStream = new FileInputStream(srcPath1);
            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(inputStream)
                    .setRequireCompress(true)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .setSFSession(sfSession)
                    .setCommand(putCommand1)
                    .setUseS3RegionalUrl(false)
                    .build());
          }

          for (SnowflakeFileTransferMetadata oneMetadata : metadatas1) {
            InputStream inputStream = new FileInputStream(srcPath1);
            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(inputStream)
                    .setRequireCompress(true)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .setSFSession(sfSession)
                    .setCommand(putCommand1)
                    .setUseS3RegionalUrl(true)
                    .build());
          }

          // Test Put file with external compression
          String putCommand2 = "put file:///dummy/path/file2.gz @" + testStageName;
          SnowflakeFileTransferAgent sfAgent2 =
              new SnowflakeFileTransferAgent(putCommand2, sfSession, new SFStatement(sfSession));
          List<SnowflakeFileTransferMetadata> metadatas2 = sfAgent2.getFileTransferMetadatas();

          String srcPath2 = getFullPathFileInResource(TEST_DATA_FILE_2);
          for (SnowflakeFileTransferMetadata oneMetadata : metadatas2) {
            String gzfilePath = destFolderCanonicalPath + "/tmp_compress.gz";
            Process p =
                Runtime.getRuntime()
                    .exec("cp -fr " + srcPath2 + " " + destFolderCanonicalPath + "/tmp_compress");
            p.waitFor();
            p = Runtime.getRuntime().exec("gzip " + destFolderCanonicalPath + "/tmp_compress");
            p.waitFor();

            InputStream gzInputStream = new FileInputStream(gzfilePath);

            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(gzInputStream)
                    .setRequireCompress(false)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .setSFSession(sfSession)
                    .setCommand(putCommand2)
                    .build());
          }

          // Download two files and verify their content.
          assertTrue(
              statement.execute(
                  "GET @"
                      + testStageName
                      + " 'file://"
                      + destFolderCanonicalPath
                      + "/' parallel=8"),
              "Failed to get files");

          // Make sure that the downloaded files are EQUAL,
          // they should be gzip compressed
          assertTrue(
              isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
          assertTrue(
              isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
        } finally {
          statement.execute("DROP STAGE if exists " + testStageName);
        }
      }
    }
  }

  /**
   * Test the streaming ingest client name and client key should be part of the file metadata in S3
   * and Azure
   */
  @Test
  @DontRunOnGithubActions
  public void testAzureS3UploadStreamingIngestFileMetadata() throws Throwable {
    String clientName = "clientName";
    String clientKey = "clientKey";
    List<String> supportedAccounts = Arrays.asList("s3testaccount", "azureaccount");
    for (String accountName : supportedAccounts) {
      try (Connection connection = getConnection(accountName);
          Statement statement = connection.createStatement()) {
        try {
          // create a stage to put the file in
          statement.execute("CREATE OR REPLACE STAGE " + testStageName);

          SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

          // Test put file with internal compression
          String putCommand = "put file:///dummy/path/file1.gz @" + testStageName;
          SnowflakeFileTransferAgent sfAgent =
              new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
          List<SnowflakeFileTransferMetadata> metadata = sfAgent.getFileTransferMetadatas();

          String srcPath1 = getFullPathFileInResource(TEST_DATA_FILE);
          for (SnowflakeFileTransferMetadata oneMetadata : metadata) {
            InputStream inputStream = new FileInputStream(srcPath1);

            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(inputStream)
                    .setRequireCompress(true)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .setSFSession(sfSession)
                    .setCommand(putCommand)
                    .setStreamingIngestClientName(clientName)
                    .setStreamingIngestClientKey(clientKey)
                    .build());

            SnowflakeStorageClient client =
                StorageClientFactory.getFactory()
                    .createClient(
                        ((SnowflakeFileTransferMetadataV1) oneMetadata).getStageInfo(),
                        1,
                        null,
                        /* session= */ null);

            String location =
                ((SnowflakeFileTransferMetadataV1) oneMetadata).getStageInfo().getLocation();
            int idx = location.indexOf('/');
            String remoteStageLocation = location.substring(0, idx);
            String path = location.substring(idx + 1) + "file1.gz";
            StorageObjectMetadata meta = client.getObjectMetadata(remoteStageLocation, path);

            // Verify that we are able to fetch the metadata
            assertEquals(clientName, client.getStreamingIngestClientName(meta));
            assertEquals(clientKey, client.getStreamingIngestClientKey(meta));
          }
        } finally {
          statement.execute("DROP STAGE if exists " + testStageName);
        }
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testNoSpaceLeftOnDeviceException() throws SQLException {
    List<String> supportedAccounts = Arrays.asList("gcpaccount", "s3testaccount", "azureaccount");
    for (String accountName : supportedAccounts) {
      try (Connection connection = getConnection(accountName)) {
        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
        try (Statement statement = connection.createStatement()) {
          try {
            SFStatement sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
            statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
            statement.execute(
                "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testPutGet_stage");
            String command = "get @testPutGet_stage/" + TEST_DATA_FILE + " 'file:///tmp'";
            SnowflakeFileTransferAgent sfAgent =
                new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
            StageInfo info = sfAgent.getStageInfo();
            SnowflakeStorageClient client =
                StorageClientFactory.getFactory().createClient(info, 1, null, /* session= */ null);

            assertThrows(
                SnowflakeSQLException.class,
                () ->
                    client.handleStorageException(
                        new StorageException(
                            client.getMaxRetries(),
                            Constants.NO_SPACE_LEFT_ON_DEVICE_ERR,
                            new IOException(Constants.NO_SPACE_LEFT_ON_DEVICE_ERR)),
                        client.getMaxRetries(),
                        "download",
                        null,
                        command,
                        null));
          } finally {
            statement.execute("DROP STAGE if exists testPutGet_stage");
          }
        }
      }
    }
  }

  @Test
  @Disabled // TODO: ignored until SNOW-1616480 is resolved
  public void testUploadWithGCSPresignedUrlWithoutConnection() throws Throwable {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    // set parameter for presignedUrl upload instead of downscoped token
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", false);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE " + testStageName);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        // Test put file with internal compression
        String putCommand = "put file:///dummy/path/file1.gz @" + testStageName;
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadata = sfAgent.getFileTransferMetadatas();

        String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
        for (SnowflakeFileTransferMetadata oneMetadata : metadata) {
          InputStream inputStream = new FileInputStream(srcPath);

          assertTrue(oneMetadata.isForOneFile());
          SnowflakeFileTransferAgent.uploadWithoutConnection(
              SnowflakeFileTransferConfig.Builder.newInstance()
                  .setSnowflakeFileTransferMetadata(oneMetadata)
                  .setUploadStream(inputStream)
                  .setRequireCompress(true)
                  .setNetworkTimeoutInMilli(0)
                  .setOcspMode(OCSPMode.FAIL_OPEN)
                  .build());
        }

        assertTrue(
            statement.execute(
                "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/' parallel=8"),
            "Failed to get files");
        assertTrue(isFileContentEqual(srcPath, false, destFolderCanonicalPath + "/file1.gz", true));
      } finally {
        statement.execute("DROP STAGE if exists " + testStageName);
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testUploadWithGCSDownscopedCredentialWithoutConnection() throws Throwable {
    uploadWithGCSDownscopedCredentialWithoutConnection();
  }

  /** Added in > 3.15.0 */
  @Test
  @DontRunOnGithubActions
  public void
      testUploadWithGCSDownscopedCredentialAndDisabledGcsDefaultCredentialsWithoutConnection()
          throws Throwable {
    System.setProperty(SnowflakeGCSClient.DISABLE_GCS_DEFAULT_CREDENTIALS_PROPERTY_NAME, "true");
    try {
      uploadWithGCSDownscopedCredentialWithoutConnection();
    } finally {
      System.clearProperty(SnowflakeGCSClient.DISABLE_GCS_DEFAULT_CREDENTIALS_PROPERTY_NAME);
    }
  }

  private void uploadWithGCSDownscopedCredentialWithoutConnection() throws Throwable {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try (Connection connection = getConnection("gcpaccount", paramProperties);
        Statement statement = connection.createStatement(); ) {
      try {
        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE " + testStageName);

        SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

        // Test put file with internal compression
        String putCommand = "put file:///dummy/path/file1.gz @" + testStageName;
        SnowflakeFileTransferAgent sfAgent =
            new SnowflakeFileTransferAgent(putCommand, sfSession, new SFStatement(sfSession));
        List<SnowflakeFileTransferMetadata> metadataList = sfAgent.getFileTransferMetadatas();
        assertEquals(1, metadataList.size());
        SnowflakeFileTransferMetadata oneMetadata = metadataList.get(0);
        assertFalse(oneMetadata.isForOneFile());

        // Upload multiple file with the same SnowflakeFileTransferMetadata
        String[] fileNames = {TEST_DATA_FILE, TEST_DATA_FILE_2};
        for (String fileName : fileNames) {
          String srcPath = getFullPathFileInResource(fileName);
          try (InputStream inputStream = new FileInputStream(srcPath)) {
            // upload file 1
            String targetFileName = fileName + ".gz";
            SnowflakeFileTransferAgent.uploadWithoutConnection(
                SnowflakeFileTransferConfig.Builder.newInstance()
                    .setSnowflakeFileTransferMetadata(oneMetadata)
                    .setUploadStream(inputStream)
                    .setDestFileName(targetFileName)
                    .setRequireCompress(true)
                    .setNetworkTimeoutInMilli(0)
                    .setOcspMode(OCSPMode.FAIL_OPEN)
                    .build());
            assertTrue(
                statement.execute(
                    "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/'"),
                "Failed to get files with down-scoped token");
            assertTrue(
                isFileContentEqual(
                    srcPath, false, destFolderCanonicalPath + "/" + targetFileName, true));
          }
        }
      } finally {
        statement.execute("DROP STAGE if exists " + testStageName);
      }
    }
  }

  /**
   * This tests that when the HTAP optimization parameter ENABLE_SNOW_654741_FOR_TESTING is set to
   * true and no session parameters or db/schema/wh are returned for select/dml statements, the
   * parameters and metadata are still accessible after creating a resultset object.
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testHTAPOptimizations() throws SQLException {
    try {
      // Set the HTAP test parameter to true
      try (Connection con = getSnowflakeAdminConnection();
          Statement statement = con.createStatement()) {
        statement.execute(
            "alter account "
                + TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT")
                + " set ENABLE_SNOW_654741_FOR_TESTING=true");
      }
      // Create a normal connection and assert that database, schema, and warehouse have expected
      // values
      try (Connection con = getConnection()) {
        SFSession session = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
        assertTrue(
            TestUtil.systemGetEnv("SNOWFLAKE_TEST_SCHEMA").equalsIgnoreCase(con.getSchema()));
        assertTrue(
            TestUtil.systemGetEnv("SNOWFLAKE_TEST_DATABASE").equalsIgnoreCase(con.getCatalog()));
        assertTrue(
            TestUtil.systemGetEnv("SNOWFLAKE_TEST_WAREHOUSE")
                .equalsIgnoreCase(session.getWarehouse()));
        try (Statement statement = con.createStatement()) {
          // Set TIMESTAMP_OUTPUT_FORMAT (which is a session parameter) to check its value later
          try {
            statement.execute(
                "alter session set TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FFTZH'");
            statement.execute("create or replace table testtable1 (cola string, colb int)");
            statement.execute(
                "insert into testtable1 values ('row1', 1), ('row2', 2), ('row3', 3)");
            try (ResultSet rs = statement.executeQuery("select * from testtable1")) {
              assertEquals(3, getSizeOfResultSet(rs));
              // Assert database, schema, and warehouse have the same values as before even though
              // the select statement will return no parameters or metadata
              assertTrue(
                  TestUtil.systemGetEnv("SNOWFLAKE_TEST_SCHEMA").equalsIgnoreCase(con.getSchema()));
              assertTrue(
                  TestUtil.systemGetEnv("SNOWFLAKE_TEST_DATABASE")
                      .equalsIgnoreCase(con.getCatalog()));
              assertTrue(
                  TestUtil.systemGetEnv("SNOWFLAKE_TEST_WAREHOUSE")
                      .equalsIgnoreCase(session.getWarehouse()));
              // Assert session parameter TIMESTAMP_OUTPUT_FORMAT has the same value as before the
              // select statement
              assertEquals(
                  "YYYY-MM-DD HH24:MI:SS.FFTZH",
                  session.getCommonParameters().get("TIMESTAMP_OUTPUT_FORMAT"));
            }
          } finally {
            statement.execute("alter session unset TIMESTAMP_OUTPUT_FORMAT");
            statement.execute("drop table if exists testtable1");
          }
        }
      }
    } finally {
      try (Connection con = getSnowflakeAdminConnection();
          Statement statement = con.createStatement()) {
        statement.execute(
            "alter account "
                + TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT")
                + " unset ENABLE_SNOW_654741_FOR_TESTING");
      }
    }
  }

  /**
   * This tests that statement parameters are still used correctly when the HTAP optimization of
   * removing all parameters is enabled.
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testHTAPStatementParameterCaching() throws SQLException {
    // Set the HTAP test parameter to true
    try (Connection con = getSnowflakeAdminConnection()) {
      Statement statement = con.createStatement();
      statement.execute(
          "alter account "
              + TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT")
              + " set ENABLE_SNOW_654741_FOR_TESTING=true");
    }
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Set up a test table with time, date, and timestamp values
      try {
        statement.execute("create or replace table timetable (t1 time, t2 timestamp, t3 date)");
        statement.execute(
            "insert into timetable values ('13:53:11', '2023-08-17 13:53:33', '2023-08-17')");
        // Set statement- level parameters that will affect the output (set output format params)
        statement
            .unwrap(SnowflakeStatement.class)
            .setParameter("TIME_OUTPUT_FORMAT", "HH12:MI:SS.FF AM");
        statement
            .unwrap(SnowflakeStatement.class)
            .setParameter("DATE_OUTPUT_FORMAT", "DD-MON-YYYY");
        statement
            .unwrap(SnowflakeStatement.class)
            .setParameter("TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD\"T\"HH24:MI:SS");
        try (ResultSet resultSet = statement.executeQuery("select * from timetable")) {
          assertTrue(resultSet.next());
          // Assert that the values match the format of the specified statement parameter output
          // format
          // values
          assertEquals("01:53:11.000000000 PM", resultSet.getString(1));
          assertEquals("2023-08-17T13:53:33", resultSet.getString(2));
          assertEquals("17-Aug-2023", resultSet.getString(3));
        }

        // Set a different statement parameter value for DATE_OUTPUT_FORMAT
        statement.unwrap(SnowflakeStatement.class).setParameter("DATE_OUTPUT_FORMAT", "MM/DD/YYYY");
        try (ResultSet resultSet = statement.executeQuery("select * from timetable")) {
          assertTrue(resultSet.next());
          // Verify it matches the new statement parameter specified output format
          assertEquals("08/17/2023", resultSet.getString(3));
        }
      } finally {
        statement.execute("drop table if exists timetable");
      }
    }
    // cleanup
    try (Connection con2 = getSnowflakeAdminConnection();
        Statement statement = con2.createStatement()) {
      statement.execute(
          "alter account "
              + TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT")
              + " unset ENABLE_SNOW_654741_FOR_TESTING");
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testS3PutInGS() throws Throwable {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    Properties paramProperties = new Properties();
    try (Connection connection = getConnection("s3testaccount", paramProperties);
        Statement statement = connection.createStatement()) {
      try {
        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE " + testStageName);

        // put file using GS system commmand, this is internal GS behavior
        final String fileName = "testFile.json";
        final String content = "testName: testS3PutInGs";
        String putSystemCall =
            String.format(
                "call system$it('PUT_FILE_TO_STAGE', '%s', '%s', '%s', '%s')",
                testStageName, fileName, content, "false");
        statement.execute(putSystemCall);

        // get file using jdbc
        String getCall =
            String.format("GET @%s 'file://%s/'", testStageName, destFolderCanonicalPath);
        statement.execute(getCall);

        InputStream downloadedFileStream =
            new FileInputStream(destFolderCanonicalPath + "/" + fileName);
        String downloadedFile = IOUtils.toString(downloadedFileStream, StandardCharsets.UTF_8);
        assertTrue(
            content.equals(downloadedFile), "downloaded content does not equal uploaded content");
      } finally {
        statement.execute("DROP STAGE if exists " + testStageName);
      }
    }
  }

  /** Added in > 3.17.0 */
  @Test
  public void shouldLoadDriverWithDisabledTelemetryOob() throws ClassNotFoundException {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    assertFalse(TelemetryService.getInstance().isEnabled());
    assertFalse(TelemetryService.getInstance().isHTAPEnabled());
  }
}
