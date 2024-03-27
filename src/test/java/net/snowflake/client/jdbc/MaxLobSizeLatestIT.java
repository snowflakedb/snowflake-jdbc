/*
 * Copyright (c) 2024 Snowflake Computing Inc. All right reserved.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.UUIDUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MaxLobSizeLatestIT extends BaseJDBCTest {

  // Max LOB size is testable from version 3.15.0 and above.
  private static int maxLobSize = 16 * 1024 * 1024;
  private static int largeLobSize = maxLobSize / 2;
  private static int mediumLobSize = largeLobSize / 2;
  private static int originLobSize = mediumLobSize / 2;
  private static int smallLobSize = 16;

  private static Map<Integer, String> LobSizeStringValues =
      new HashMap<Integer, String>() {
        {
          put(smallLobSize, generateRandomString(smallLobSize));
          put(originLobSize, generateRandomString(originLobSize));
          put(mediumLobSize, generateRandomString(mediumLobSize));
          put(largeLobSize, generateRandomString(largeLobSize));
          put(maxLobSize, generateRandomString(maxLobSize));
        }
      };

  @BeforeClass
  public static void setUp() {
    System.setProperty(
        // the max json string should be ~1.33 for Arrow response so let's use 1.5 to be sure
        ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, Integer.toString((int) (maxLobSize * 1.5)));
  }

  @Parameterized.Parameters(name = "lobSize={0}, resultFormat={1}")
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

  public MaxLobSizeLatestIT(int lobSize, String resultFormat) throws SQLException {
    this.lobSize = lobSize;
    this.resultFormat = resultFormat;

    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      createTable(lobSize, stmt);
    }
  }

  private static String tableName = "my_lob_test";
  private static String executeInsert = "insert into " + tableName + " (c1, c2, c3) values (";
  private static String executePreparedStatementInsert = executeInsert + "?, ?, ?)";
  private static String selectQuery = "select * from " + tableName + " where c3=";

  private static String generateRandomString(int stringSize) {
    RandomStringGenerator randomStringGenerator =
        new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    return randomStringGenerator.generate(stringSize);
  }

  private static void setResultFormat(Statement stmt, String format) throws SQLException {
    stmt.execute("alter session set jdbc_query_result_format = '" + format + "'");
  }

  private void createTable(int lobSize, Statement stmt) throws SQLException {
    String createTableQuery =
        "create or replace table "
            + tableName
            + " (c1 varchar, c2 varchar("
            + lobSize
            + "), c3 varchar)";
    stmt.execute(createTableQuery);
  }

  private void insertQuery(String varCharValue, String uuidValue, Statement stmt)
      throws SQLException {
    stmt.executeUpdate(executeInsert + "'abc', '" + varCharValue + "', '" + uuidValue + "')");
  }

  private void preparedInsertQuery(String varCharValue, String uuidValue, Connection con)
      throws SQLException {
    try (PreparedStatement pstmt = con.prepareStatement(executePreparedStatementInsert)) {
      pstmt.setString(1, "abc");
      pstmt.setString(2, varCharValue);
      pstmt.setString(3, uuidValue);

      pstmt.execute();
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("Drop table if exists " + tableName);
    }
  }

  @Test
  public void testStandardInsertAndSelectWithMaxLobSizeEnabled() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      setResultFormat(stmt, resultFormat);

      String varCharValue = LobSizeStringValues.get(lobSize);
      String uuidValue = UUIDUtils.getUUID().toString();
      insertQuery(varCharValue, uuidValue, stmt);

      try (ResultSet rs = stmt.executeQuery(selectQuery + "'" + uuidValue + "'")) {
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals(varCharValue, rs.getString(2));
        assertEquals(uuidValue, rs.getString(3));
      }
    }
  }

  @Test
  public void testPreparedInsertWithMaxLobSizeEnabled() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      setResultFormat(stmt, resultFormat);

      String maxVarCharValue = LobSizeStringValues.get(lobSize);
      String uuidValue = UUIDUtils.getUUID().toString();
      preparedInsertQuery(maxVarCharValue, uuidValue, con);

      try (ResultSet rs = stmt.executeQuery(selectQuery + "'" + uuidValue + "'")) {
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals(maxVarCharValue, rs.getString(2));
        assertEquals(uuidValue, rs.getString(3));
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

    String varCharValue = LobSizeStringValues.get(lobSize);
    String uuidValue = UUIDUtils.getUUID().toString();
    String fileInput = "abc," + varCharValue + "," + uuidValue;

    // Print data to new temporary file
    try (PrintWriter out = new PrintWriter(filePath)) {
      out.println(fileInput);
    }

    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      setResultFormat(stmt, resultFormat);

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
      try (ResultSet rsCopy = stmt.executeQuery(selectQuery + "'" + uuidValue + "'")) {
        assertTrue(rsCopy.next());
        assertEquals("abc", rsCopy.getString(1));
        assertEquals(varCharValue, rsCopy.getString(2));
        assertEquals(uuidValue, rsCopy.getString(3));
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
