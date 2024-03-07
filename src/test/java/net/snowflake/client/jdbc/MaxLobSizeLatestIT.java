/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import net.snowflake.client.core.ObjectMapperFactory;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MaxLobSizeLatestIT extends BaseJDBCTest {

  // Max LOB size is testable from version 3.15.0 and above.
  // TODO: increase max size to 128 * 1024 * 1024
  private static int maxLobSize = 16 * 1024 * 1024;
  private static int largeLobSize = maxLobSize / 2;
  private static int mediumLobSize = largeLobSize / 2;
  private static int originLobSize = mediumLobSize / 2;
  private static int smallLobSize = 16;

  @BeforeClass
  public static void setUp() {
    // TODO: May need to increase MAX_JSON_STRING_LENGTH_JVM once we increase the max LOB size.
    System.setProperty(
        ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, Integer.toString(45_000_000));
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    int[] lobSizes =
        new int[] {smallLobSize, originLobSize, mediumLobSize, largeLobSize, maxLobSize};
    String[] resultFormats = new String[] {"Arrow", "JSON"};
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < lobSizes.length; i++) {
      for (int j = 0; j < resultFormats.length; j++) {
        ret.add(new Object[] {lobSizes[i], resultFormats[j]});
      }
    }
    return ret;
  }

  private final int lobSize;

  private final String resultFormat;

  public MaxLobSizeLatestIT(int lobSize, String resultFormat) {
    this.lobSize = lobSize;
    this.resultFormat = resultFormat;
  }

  private static String tableName = "my_lob_test";
  private static String enableMaxLobSize =
      "alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY = 'ENABLED'";
  private static String executeInsert = "insert into " + tableName + " (c1, c2, c3) values (";
  private static String executePreparedStatementInsert = executeInsert + "?, ?, ?)";
  private static String selectQuery = "select * from " + tableName;

  private static String generateRandomString(int stringSize) {
    RandomStringGenerator randomStringGenerator =
        new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    return randomStringGenerator.generate(stringSize);
  }

  private static void setResultFormat(Connection con, String format) throws SQLException {
    try (Statement stmt = con.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + format + "'");
    }
  }

  private void createTable(int lobSize, Connection con) throws SQLException {
    try (Statement stmt = con.createStatement()) {
      String createTableQuery =
          "create or replace table "
              + tableName
              + " (c1 varchar, c2 varchar("
              + lobSize
              + "), c3 int)";
      stmt.execute(createTableQuery);
    }
  }

  private void insertQuery(String varCharValue, int intValue, Connection con) throws SQLException {
    try (Statement stmt = con.createStatement()) {
      stmt.executeUpdate(
          executeInsert + "'" + varCharValue + "', '" + varCharValue + "', " + intValue + ")");
    }
  }

  private void preparedInsertQuery(String varCharValue, int intValue, Connection con)
      throws SQLException {
    try (PreparedStatement pstmt = con.prepareStatement(executePreparedStatementInsert)) {
      pstmt.setString(1, varCharValue);
      pstmt.setString(2, varCharValue);
      pstmt.setInt(3, intValue);

      pstmt.execute();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("Drop table if exists " + tableName);
    }
  }

  @Test
  public void testStandardInsertAndSelectWithMaxLobSizeEnabled() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      // TODO: Uncomment when ready to test with max lob size enabled.
      // stmt.execute(enableMaxLobSize);

      setResultFormat(con, resultFormat);
      createTable(lobSize, con);

      String varCharValue = generateRandomString(lobSize);
      int intValue = new Random().nextInt();
      insertQuery(varCharValue, intValue, con);

      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        rs.next();
        assertEquals(varCharValue, rs.getString(1));
        assertEquals(varCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
      }
    }
  }

  @Test
  public void testPreparedInsertWithMaxLobSizeEnabled() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      // TODO: Uncomment when ready to test with max lob size enabled.
      // stmt.execute(enableMaxLobSize);

      setResultFormat(con, resultFormat);
      createTable(lobSize, con);

      String maxVarCharValue = generateRandomString(lobSize);
      int intValue = new Random().nextInt();
      preparedInsertQuery(maxVarCharValue, intValue, con);

      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        rs.next();
        assertEquals(maxVarCharValue, rs.getString(1));
        assertEquals(maxVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
      }
    }
  }

  @Test
  public void testPutAndGet() throws IOException, SQLException {
    File tempFile = File.createTempFile("LobSizeTest", ".csv");
    // Delete file when JVM shuts down
    tempFile.deleteOnExit();

    String filePath = tempFile.getPath();
    String filePathEscaped = filePath.replace("\\", "\\\\");
    String fileName = tempFile.getName();

    String varCharValue = generateRandomString(lobSize);
    int intValue = new Random().nextInt();
    String fileInput = varCharValue + "," + varCharValue + "," + intValue;

    // Print data to new temporary file
    try (PrintWriter out = new PrintWriter(filePath)) {
      out.println(fileInput);
    }

    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      // TODO: Uncomment when ready to test with max lob size enabled.
      // stmt.execute(enableMaxLobSize);

      createTable(lobSize, con);

      // Test PUT
      String sqlPut = "PUT 'file://" + filePathEscaped + "' @%" + tableName;

      stmt.execute(sqlPut);

      try (ResultSet rsPut = stmt.getResultSet()) {
        assertTrue(rsPut.next());
        assertEquals(fileName, rsPut.getString(1));
        assertEquals(fileName + ".gz", rsPut.getString(2));
        assertEquals("GZIP", rsPut.getString(6));
        assertEquals("UPLOADED", rsPut.getString(7));
      }

      try (ResultSet rsFiles = stmt.executeQuery("ls @%" + tableName)) {
        // ResultSet should return a row with the zipped file name
        assertTrue(rsFiles.next());
        assertEquals(fileName + ".gz", rsFiles.getString(1));
      }

      String copyInto =
          "copy into "
              + tableName
              + " from @%"
              + tableName
              + " file_format=(type=csv compression='gzip')";
      stmt.execute(copyInto);

      // Check that results are copied into table correctly
      try (ResultSet rsCopy = stmt.executeQuery("Select * from " + tableName)) {
        assertTrue(rsCopy.next());
        assertEquals(varCharValue, rsCopy.getString(1));
        assertEquals(varCharValue, rsCopy.getString(2));
        assertEquals(intValue, rsCopy.getInt(3));
      }

      // Test Get
      Path tempDir = Files.createTempDirectory("MaxLobTest");
      // Delete tempDir when JVM shuts down
      tempDir.toFile().deleteOnExit();
      String pathToTempDir = tempDir.toString().replace("\\", "\\\\");

      String getSql = "get @%" + tableName + " 'file://" + pathToTempDir + "'";
      stmt.execute(getSql);

      try (ResultSet rsGet = stmt.getResultSet()) {
        assertTrue(rsGet.next());
        assertEquals(fileName + ".gz", rsGet.getString(1));
        assertEquals("DOWNLOADED", rsGet.getString(3));
        assertEquals("DECRYPTED", rsGet.getString(4));
      }
    }
  }
}
