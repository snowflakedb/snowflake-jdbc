/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeDriverIT.findFile;
import static org.junit.Assert.*;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.*;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * General JDBC tests for the latest JDBC driver. This doesn't work for the oldest supported driver.
 * Revisit this tests whenever bumping up the oldest supported driver to examine if the tests still
 * is not applicable. If it is applicable, move tests to SnowflakeDriverIT so that both the latest
 * and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class SnowflakeDriverLatestIT extends BaseJDBCTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public TemporaryFolder tmpFolder2 = new TemporaryFolder();

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
  public void testGetSessionID() throws Throwable {
    Connection con = getConnection();
    String sessionID = con.unwrap(SnowflakeConnection.class).getSessionID();
    Statement statement = con.createStatement();
    ResultSet rset = statement.executeQuery("select current_session()");
    rset.next();
    assertEquals(sessionID, rset.getString(1));
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutThreshold() throws SQLException {
    try (Connection connection = getConnection()) {
      // assert that threshold equals default 200 from server side
      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      Statement statement = connection.createStatement();
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
      try {
        agent = new SnowflakeFileTransferAgent(commandWithInvalidThreshold, sfSession, sfStatement);
      }
      // assert invalid value causes exception to be thrown of type INVALID_PARAMETER_VALUE
      catch (SQLException e) {
        assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
      }
      statement.close();
    } catch (SQLException ex) {
      throw ex;
    }
  }

  /** Test API for Spark connector for FileTransferMetadata */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGCPFileTransferMetadataWithOneFile() throws Throwable {
    Connection connection = null;
    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    try {
      connection = getConnection("gcpaccount");
      Statement statement = connection.createStatement();

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

        assert (oneMetadata.isForOneFile());
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
        assert (oneMetadata.isForOneFile());
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
          "Failed to get files",
          statement.execute(
              "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/' parallel=8"));

      // Make sure that the downloaded files are EQUAL,
      // they should be gzip compressed
      assert (isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
      assert (isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + testStageName);
        connection.close();
      }
    }
  }

  /** Test API for Kafka connector for FileTransferMetadata */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAzureS3FileTransferMetadataWithOneFile() throws Throwable {
    Connection connection = null;
    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();

    List<String> supportedAaccounts = Arrays.asList("s3testaccount", "azureaccount");
    for (String accountName : supportedAaccounts) {
      try {
        connection = getConnection(accountName);
        Statement statement = connection.createStatement();

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
            "Failed to get files",
            statement.execute(
                "GET @" + testStageName + " 'file://" + destFolderCanonicalPath + "/' parallel=8"));

        // Make sure that the downloaded files are EQUAL,
        // they should be gzip compressed
        assert (isFileContentEqual(srcPath1, false, destFolderCanonicalPath + "/file1.gz", true));
        assert (isFileContentEqual(srcPath2, false, destFolderCanonicalPath + "/file2.gz", true));
      } finally {
        if (connection != null) {
          connection.createStatement().execute("DROP STAGE if exists " + testStageName);
          connection.close();
        }
      }
    }
  }

  /** Negative test for FileTransferMetadata. It is only supported for PUT. */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGCPFileTransferMetadataNetativeOnlySupportPut() throws Throwable {
    Connection connection = null;
    int expectExceptionCount = 1;
    int actualExceptionCount = -1;
    try {
      connection = getConnection("gcpaccount");
      Statement statement = connection.createStatement();

      // create a stage to put the file in
      statement.execute("CREATE OR REPLACE STAGE " + testStageName);

      // Put one file to the stage
      String srcPath = getFullPathFileInResource(TEST_DATA_FILE);
      statement.execute("put file://" + srcPath + " @" + testStageName);

      SFSession sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();

      File destFolder = tmpFolder.newFolder();
      String destFolderCanonicalPath = destFolder.getCanonicalPath();

      String getCommand = "get @" + testStageName + " file://" + destFolderCanonicalPath;

      // The GET can be executed in normal way.
      statement.execute(getCommand);

      // Start negative test for GET.
      SnowflakeFileTransferAgent sfAgent =
          new SnowflakeFileTransferAgent(getCommand, sfSession, new SFStatement(sfSession));

      // Below function call should fail.
      actualExceptionCount = 0;
      sfAgent.getFileTransferMetadatas();
      fail("Above function should raise exception for GET");
    } catch (Exception ex) {
      System.out.println("Negative test to hit expected exception: " + ex.getMessage());
      actualExceptionCount++;
    } finally {
      if (connection != null) {
        connection.createStatement().execute("DROP STAGE if exists " + testStageName);
        connection.close();
      }
    }
    assertEquals(expectExceptionCount, actualExceptionCount);
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
    try {
      url = "snowflake.reg.local:8082";
      driver.getPropertyInfo(url, props);
    } catch (SQLException e) {
      assertEquals((int) ErrorCode.INVALID_CONNECT_STRING.getMessageCode(), e.getErrorCode());
    }
  }

  /**
   * Tests put with overwrite set to false. Require SNOW-206907 to be merged to pass tests for gcp
   * account
   *
   * @throws Throwable
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutOverwriteFalseNoDigest() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    // create 2 files: an original, and one that will overwrite the original
    File file1 = tmpFolder.newFile("testfile.csv");
    BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
    bw.write("Writing original file content. This should get overwritten.");
    bw.close();

    File file2 = tmpFolder2.newFile("testfile.csv");
    bw = new BufferedWriter(new FileWriter(file2));
    bw.write("This is all new! This should be the result of the overwriting.");
    bw.close();

    String sourceFilePathOriginal = file1.getCanonicalPath();
    String sourceFilePathOverwrite = file2.getCanonicalPath();

    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try {
        connection = getConnection(accounts.get(i), paramProperties);

        statement = connection.createStatement();

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE testing_stage");
        assertTrue(
            "Failed to put a file",
            statement.execute("PUT file://" + sourceFilePathOriginal + " @testing_stage"));
        // check that file exists in stage after PUT
        findFile(statement, "ls @testing_stage/");

        // put another file in same stage with same filename with overwrite = true
        assertTrue(
            "Failed to put a file",
            statement.execute(
                "PUT file://" + sourceFilePathOverwrite + " @testing_stage overwrite=false"));

        // check that file exists in stage after PUT
        findFile(statement, "ls @testing_stage/");

        // get file from new stage
        assertTrue(
            "Failed to get files",
            statement.execute(
                "GET @testing_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

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
        statement.close();
      }
    }
  }

  /**
   * Test NULL in LIMIT and OFFSET with Snow-76376 enabled this should be handled as without LIMIT
   * and OFFSET
   */
  @Test
  public void testSnow76376() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();
      regularStatement = connection.createStatement();
      regularStatement.execute(
          "create or replace table t(a int) as select * from values" + "(1),(2),(8),(10)");

      preparedStatement =
          connection.prepareStatement("SELECT * FROM t " + "ORDER BY a LIMIT " + "? OFFSET ?");

      ////////////////////////////
      // both NULL
      preparedStatement.setNull(1, 4); // int
      preparedStatement.setNull(2, 4); // int

      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        resultSet.next();
        assertEquals(1, resultSet.getInt(1));
        resultSet.next();
        assertEquals(2, resultSet.getInt(1));
        resultSet.next();
        assertEquals(8, resultSet.getInt(1));
        resultSet.next();
        assertEquals(10, resultSet.getInt(1));
      } else {
        fail("Could not execute preparedStatement with OFFSET and LIMIT set " + "to NULL");
      }

      ////////////////////////////
      // both empty string
      preparedStatement.setString(1, "");
      preparedStatement.setString(2, "");

      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        resultSet.next();
        assertEquals(1, resultSet.getInt(1));
        resultSet.next();
        assertEquals(2, resultSet.getInt(1));
        resultSet.next();
        assertEquals(8, resultSet.getInt(1));
        resultSet.next();
        assertEquals(10, resultSet.getInt(1));
      } else {
        fail("Could not execute preparedStatement with OFFSET and LIMIT set " + "to empty string");
      }

      ////////////////////////////
      // only LIMIT NULL
      preparedStatement.setNull(1, 4); // int
      preparedStatement.setInt(2, 2);

      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        resultSet.next();
        assertEquals(8, resultSet.getInt(1));
        resultSet.next();
        assertEquals(10, resultSet.getInt(1));
      } else {
        fail("Could not execute preparedStatement with LIMIT set to NULL");
      }

      ////////////////////////////
      // only LIMIT empty string
      preparedStatement.setString(1, "");
      preparedStatement.setInt(2, 2);

      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        resultSet.next();
        assertEquals(8, resultSet.getInt(1));
        resultSet.next();
        assertEquals(10, resultSet.getInt(1));
      } else {
        fail("Could not execute preparedStatement with LIMIT set to empty " + "string");
      }

      ////////////////////////////
      // only OFFSET NULL
      preparedStatement.setInt(1, 3); // int
      preparedStatement.setNull(2, 4);

      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        resultSet.next();
        assertEquals(1, resultSet.getInt(1));
        resultSet.next();
        assertEquals(2, resultSet.getInt(1));
        resultSet.next();
        assertEquals(8, resultSet.getInt(1));
      } else {
        fail("Could not execute preparedStatement with OFFSET set to NULL");
      }

      ////////////////////////////
      // only OFFSET empty string
      preparedStatement.setInt(1, 3); // int
      preparedStatement.setNull(2, 4);

      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        resultSet.next();
        assertEquals(1, resultSet.getInt(1));
        resultSet.next();
        assertEquals(2, resultSet.getInt(1));
        resultSet.next();
        assertEquals(8, resultSet.getInt(1));
      } else {
        fail("Could not execute preparedStatement with OFFSET set to empty " + "string");
      }

      ////////////////////////////
      // OFFSET and LIMIT NULL for constant select query
      preparedStatement =
          connection.prepareStatement("SELECT 1 FROM t " + "ORDER BY a LIMIT " + "? OFFSET ?");
      preparedStatement.setNull(1, 4); // int
      preparedStatement.setNull(2, 4); // int
      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        for (int i = 0; i < 4; i++) {
          resultSet.next();
          assertEquals(1, resultSet.getInt(1));
        }
      } else {
        fail("Could not execute constant preparedStatement with OFFSET and " + "LIMIT set to NULL");
      }

      ////////////////////////////
      // OFFSET and LIMIT empty string for constant select query
      preparedStatement.setString(1, ""); // int
      preparedStatement.setString(2, ""); // int
      if (preparedStatement.execute()) {
        resultSet = preparedStatement.getResultSet();
        for (int i = 0; i < 4; i++) {
          resultSet.next();
          assertEquals(1, resultSet.getInt(1));
        }
      } else {
        fail(
            "Could not execute constant preparedStatement with OFFSET and "
                + "LIMIT set to empty string");
      }

    } finally {
      if (regularStatement != null) {
        regularStatement.execute("drop table t");
        regularStatement.close();
      }

      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  /**
   * Tests that result columns of type GEOGRAPHY appear as VARCHAR / VARIANT / BINARY to the client,
   * depending on the value of GEOGRAPHY_OUTPUT_FORMAT
   *
   * @throws Throwable
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGeoOutputTypes() throws Throwable {
    Connection connection = null;
    Statement regularStatement = null;

    try {
      Properties paramProperties = new Properties();

      paramProperties.put("ENABLE_USER_DEFINED_TYPE_EXPANSION", true);
      paramProperties.put("ENABLE_GEOGRAPHY_TYPE", true);

      connection = getConnection(paramProperties);

      regularStatement = connection.createStatement();

      regularStatement.execute("create or replace table t_geo(geo geography);");

      regularStatement.execute("insert into t_geo values ('POINT(0 0)'), ('LINESTRING(1 1, 2 2)')");

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
      if (regularStatement != null) {
        regularStatement.execute("drop table t_geo");
        regularStatement.close();
      }

      if (connection != null) {
        connection.close();
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
    ResultSet resultSet = null;

    try {
      regularStatement.execute("alter session set GEOGRAPHY_OUTPUT_FORMAT='" + outputFormat + "'");

      regularStatement.execute(
          "alter session set ENABLE_UDT_EXTERNAL_TYPE_NAMES=" + enableExternalTypeNames);

      resultSet = regularStatement.executeQuery("select * from t_geo");

      ResultSetMetaData metadata = resultSet.getMetaData();

      assertEquals(1, metadata.getColumnCount());

      // GeoJSON: SQL type OBJECT, Java type String
      assertEquals(expectedColumnTypeName, metadata.getColumnTypeName(1));
      assertEquals(expectedColumnClassName, metadata.getColumnClassName(1));
      assertEquals(expectedColumnType, metadata.getColumnType(1));

    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGeoMetadata() throws Throwable {
    Connection connection = null;
    Statement regularStatement = null;

    try {
      Properties paramProperties = new Properties();

      paramProperties.put("ENABLE_FIX_182763", true);

      connection = getConnection(paramProperties);

      regularStatement = connection.createStatement();

      regularStatement.execute("create or replace table t_geo(geo geography);");

      testGeoMetadataSingle(connection, regularStatement, "geoJson", Types.VARCHAR);

      testGeoMetadataSingle(connection, regularStatement, "geoJson", Types.VARCHAR);

      testGeoMetadataSingle(connection, regularStatement, "wkt", Types.VARCHAR);

      testGeoMetadataSingle(connection, regularStatement, "wkt", Types.VARCHAR);

      testGeoMetadataSingle(connection, regularStatement, "wkb", Types.BINARY);

      testGeoMetadataSingle(connection, regularStatement, "wkb", Types.BINARY);

    } finally {
      if (regularStatement != null) {
        regularStatement.execute("drop table t_geo");
        regularStatement.close();
      }

      if (connection != null) {
        connection.close();
      }
    }
  }

  private void testGeoMetadataSingle(
      Connection connection,
      Statement regularStatement,
      String outputFormat,
      int expectedColumnType)
      throws Throwable {
    ResultSet resultSet = null;

    try {
      regularStatement.execute("alter session set GEOGRAPHY_OUTPUT_FORMAT='" + outputFormat + "'");

      DatabaseMetaData md = connection.getMetaData();
      resultSet = md.getColumns(null, null, "T_GEO", null);
      ResultSetMetaData metadata = resultSet.getMetaData();

      assertEquals(24, metadata.getColumnCount());

      assertTrue(resultSet.next());

      assertEquals(expectedColumnType, resultSet.getInt(5));
      assertEquals("GEOGRAPHY", resultSet.getString(6));
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
  }
  /**
   * Tests that upload and download small file to gcs stage. Require SNOW-206907 to be merged (still
   * pass when not merge, but will use presigned url instead of GoogleCredential)
   *
   * @throws Throwable
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutGetGcsDownscopedCredential() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    try {
      connection = getConnection("gcpaccount", paramProperties);

      statement = connection.createStatement();

      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);

      File destFolder = tmpFolder.newFolder();
      String destFolderCanonicalPath = destFolder.getCanonicalPath();
      String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

      try {
        statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");

        assertTrue(
            "Failed to put a file",
            statement.execute("PUT file://" + sourceFilePath + " @testPutGet_stage"));

        findFile(statement, "ls @testPutGet_stage/");

        // download the file we just uploaded to stage
        assertTrue(
            "Failed to get a file",
            statement.execute(
                "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

        // Make sure that the downloaded file exists, it should be gzip compressed
        File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
        assert (downloaded.exists());

        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
        p.waitFor();

        File original = new File(sourceFilePath);
        File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE);
        assert (original.length() == unzipped.length());
      } finally {
        statement.execute("DROP STAGE IF EXISTS testGetPut_stage");
        statement.close();
      }
    } finally {
      closeSQLObjects(null, statement, connection);
    }
  }

  /**
   * Tests that upload and download big file to gcs stage. Require SNOW-206907 to be merged (still
   * pass when not merge, but will use presigned url instead of GoogleCredential)
   *
   * @throws Throwable
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutGetLargeFileGCSDownscopedCredential() throws Throwable {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    Connection connection = getConnection("gcpaccount", paramProperties);
    Statement statement = connection.createStatement();

    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    File largeTempFile = tmpFolder.newFile("largeFile.csv");
    BufferedWriter bw = new BufferedWriter(new FileWriter(largeTempFile));
    bw.write("Creating large test file for GCP PUT/GET test");
    bw.write(System.lineSeparator());
    bw.write("Creating large test file for GCP PUT/GET test");
    bw.write(System.lineSeparator());
    bw.close();
    File largeTempFile2 = tmpFolder.newFile("largeFile2.csv");

    String sourceFilePath = largeTempFile.getCanonicalPath();

    try {
      // copy info from 1 file to another and continue doubling file size until we reach ~1.5GB,
      // which is a large file
      for (int i = 0; i < 12; i++) {
        copyContentFrom(largeTempFile, largeTempFile2);
        copyContentFrom(largeTempFile2, largeTempFile);
      }

      // create a stage to put the file in
      statement.execute("CREATE OR REPLACE STAGE largefile_stage");
      assertTrue(
          "Failed to put a file",
          statement.execute("PUT file://" + sourceFilePath + " @largefile_stage"));

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
          "Failed to get files",
          statement.execute(
              "GET @extra_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

      // Make sure that the downloaded file exists; it should be gzip compressed
      File downloaded = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
      assert (downloaded.exists());

      // unzip the file
      Process p =
          Runtime.getRuntime()
              .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
      p.waitFor();

      // compare the original file with the file that's been uploaded, copied into a table, copied
      // back into a stage,
      // downloaded, and unzipped
      File unzipped = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv");
      assert (largeTempFile.length() == unzipped.length());
      assert (FileUtils.contentEquals(largeTempFile, unzipped));
    } finally {
      statement.execute("DROP STAGE IF EXISTS largefile_stage");
      statement.execute("DROP STAGE IF EXISTS extra_stage");
      statement.execute("DROP TABLE IF EXISTS large_table");
      statement.close();
      connection.close();
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
    FileChannel fIn = inputStream.getChannel();
    FileChannel fOut = outputStream.getChannel();
    fOut.transferFrom(fIn, 0, fIn.size());
    fIn.position(0);
    fOut.transferFrom(fIn, fIn.size(), fIn.size());
    fOut.close();
    fIn.close();
  }
}
