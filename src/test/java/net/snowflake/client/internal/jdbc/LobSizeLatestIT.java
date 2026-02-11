package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import net.snowflake.client.annotations.DontRunOnJenkins;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.ObjectMapperFactory;
import net.snowflake.client.internal.core.UUIDUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class LobSizeLatestIT extends BaseJDBCTest {

  private static final Logger logger = Logger.getLogger(SnowflakeDriverIT.class.getName());
  private static final Map<Integer, String> LobSizeStringValues = new HashMap<>();

  // Max LOB size is testable from version 3.15.0 and above.
  private static int maxLobSize = 16 * 1024 * 1024; // default value
  private static int largeLobSize = maxLobSize / 2;
  private static int mediumLobSize = largeLobSize / 2;
  private static int smallLobSize = 16;
  private static int originLobSize = 16 * 1024 * 1024;

  @BeforeAll
  public static void setUp() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection()) {
      // get max LOB size from session
      maxLobSize = con.getMetaData().getMaxCharLiteralLength();
      logger.log(Level.INFO, "Using max lob size: " + maxLobSize);
      System.setProperty(
          // the max json string should be ~1.33 for Arrow response so let's use 1.5 to be sure
          ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM,
          Integer.toString((int) (maxLobSize * 1.5)));
      LobSizeStringValues.put(smallLobSize, generateRandomString(smallLobSize));
      LobSizeStringValues.put(originLobSize, generateRandomString(originLobSize));
      LobSizeStringValues.put(mediumLobSize, generateRandomString(mediumLobSize));
      LobSizeStringValues.put(largeLobSize, generateRandomString(largeLobSize));
      LobSizeStringValues.put(maxLobSize, generateRandomString(maxLobSize));
    }
  }

  static class DataProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      int[] lobSizes =
          new int[] {smallLobSize, originLobSize, mediumLobSize, largeLobSize, maxLobSize};
      String[] resultFormats = new String[] {"Arrow", "JSON"};
      List<Arguments> ret = new ArrayList<>();
      for (int size : lobSizes) {
        for (String format : resultFormats) {
          ret.add(Arguments.of(size, format));
        }
      }
      return ret.stream();
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

  @AfterAll
  public static void tearDown() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("Drop table if exists " + tableName);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnJenkins // the MxLobParameters isn't configured properly on new environment
  public void testStandardInsertAndSelectWithMaxLobSizeEnabled(int lobSize, String resultFormat)
      throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      createTable(lobSize, stmt);
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

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnJenkins // the MxLobParameters isn't configured properly on new environment
  public void testPreparedInsertWithMaxLobSizeEnabled(int lobSize, String resultFormat)
      throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      createTable(lobSize, stmt);
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

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  @DontRunOnJenkins // the MxLobParameters isn't configured properly on new environment
  public void testPutAndGet(int lobSize, String resultFormat) throws IOException, SQLException {
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
      createTable(lobSize, stmt);
      setResultFormat(stmt, resultFormat);
      if (lobSize > originLobSize) { // for increased LOB size (16MB < lobSize < 128MB)
        stmt.execute("alter session set ALLOW_LARGE_LOBS_IN_EXTERNAL_SCAN = true");
      }
      // Test PUT (GZIP disabled to isolate encrypt/upload path)
      String sqlPut =
          "PUT 'file://" + filePathEscaped + "' @%" + tableName + " AUTO_COMPRESS=FALSE";

      stmt.execute(sqlPut);

      try (ResultSet rsPut = stmt.getResultSet()) {
        assertTrue(rsPut.next());
        assertEquals(fileName, rsPut.getString(1));
        assertEquals(fileName, rsPut.getString(2));
        assertEquals("NONE", rsPut.getString(6));
        assertEquals("UPLOADED", rsPut.getString(7));
      }

      // --- Diagnostic: log original file size ---
      long originalFileSize = tempFile.length();
      System.out.println(
          "[LobSizeTest Diagnostic] PUT completed. lobSize="
              + lobSize
              + " resultFormat="
              + resultFormat
              + " originalFile="
              + fileName
              + " originalSize="
              + originalFileSize
              + " bytes");

      // --- Post-upload size verification via LS ---
      long stagedFileSize;
      try (ResultSet rsFiles = stmt.executeQuery("ls @%" + tableName)) {
        assertTrue(rsFiles.next());
        assertEquals(fileName, rsFiles.getString(1));
        stagedFileSize = rsFiles.getLong(2);
        System.out.println(
            "[LobSizeTest Diagnostic] Staged file: "
                + rsFiles.getString(1)
                + " stagedSize="
                + stagedFileSize
                + " bytes");
      }
      assertTrue(
          stagedFileSize > 0, "Staged file size should be positive but was " + stagedFileSize);

      // --- Non-fatal integrity verification (no GZIP -- raw CSV through encrypt/upload) ---
      // All checks are collected as warnings so the test always proceeds to COPY INTO.
      StringBuilder verificationErrors = new StringBuilder();

      String originalDigest = sha256Hex(tempFile);
      System.out.println("[LobSizeTest Diagnostic] Original file SHA-256: " + originalDigest);

      // Check the size gap between LS (encrypted) and GET (decrypted) for AES-CBC padding
      long expectedPadding = 16 - (originalFileSize % 16);
      long sizeDiff = stagedFileSize - originalFileSize;
      System.out.println(
          "[LobSizeTest Diagnostic] stagedSize="
              + stagedFileSize
              + " originalSize="
              + originalFileSize
              + " sizeDiff="
              + sizeDiff
              + " expectedAESPadding="
              + expectedPadding);
      if (sizeDiff != expectedPadding) {
        String msg =
            "sizeDiff ("
                + sizeDiff
                + ") != expectedAESPadding ("
                + expectedPadding
                + "). Possible corruption during upload or download.";
        System.out.println("[LobSizeTest Diagnostic] WARNING: " + msg);
        verificationErrors.append(msg).append("\n");
      }

      // Download file from stage (1st GET)
      Path verifyDir = Files.createTempDirectory("LobSizeVerify");
      verifyDir.toFile().deleteOnExit();
      String verifyDirEscaped = verifyDir.toString().replace("\\", "\\\\");

      stmt.execute("get @%" + tableName + " 'file://" + verifyDirEscaped + "'");
      try (ResultSet rsVerify = stmt.getResultSet()) {
        assertTrue(rsVerify.next(), "GET for verification should return a result");
        assertEquals("DOWNLOADED", rsVerify.getString(3), "GET for verification should succeed");
      }

      File downloadedFile = new File(verifyDir.toFile(), fileName);
      assertTrue(
          downloadedFile.exists(),
          "Downloaded file should exist at: " + downloadedFile.getAbsolutePath());
      long downloadedSize = downloadedFile.length();
      String downloadDigest1 = sha256Hex(downloadedFile);
      System.out.println(
          "[LobSizeTest Diagnostic] Downloaded file (1st GET): size="
              + downloadedSize
              + " SHA-256="
              + downloadDigest1);

      if (downloadedSize != originalFileSize) {
        String msg =
            "Size mismatch: downloaded=" + downloadedSize + " original=" + originalFileSize;
        System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
        verificationErrors.append(msg).append("\n");
      }
      if (!originalDigest.equals(downloadDigest1)) {
        // Dump first 32 bytes for diagnosis
        try (FileInputStream headerCheck = new FileInputStream(downloadedFile)) {
          byte[] head = new byte[32];
          int nRead = headerCheck.read(head, 0, head.length);
          StringBuilder hexDump = new StringBuilder();
          for (int i = 0; i < Math.max(0, nRead); i++) {
            hexDump.append(String.format("%02x ", head[i] & 0xff));
          }
          String msg =
              "SHA-256 MISMATCH: original="
                  + originalDigest
                  + " downloaded="
                  + downloadDigest1
                  + " firstBytes=["
                  + hexDump.toString().trim()
                  + "]. Data corrupted during encrypt/upload/decrypt.";
          System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
          verificationErrors.append(msg).append("\n");
        }
      } else {
        System.out.println(
            "[LobSizeTest Diagnostic] SHA-256 match: downloaded file is byte-identical"
                + " to original.");
      }

      // Second independent download to detect non-deterministic S3/decryption issues
      Path verifyDir2 = Files.createTempDirectory("LobSizeVerify2");
      verifyDir2.toFile().deleteOnExit();
      String verifyDir2Escaped = verifyDir2.toString().replace("\\", "\\\\");

      stmt.execute("get @%" + tableName + " 'file://" + verifyDir2Escaped + "'");
      try (ResultSet rsVerify2 = stmt.getResultSet()) {
        assertTrue(rsVerify2.next(), "2nd GET for verification should return a result");
        assertEquals(
            "DOWNLOADED", rsVerify2.getString(3), "2nd GET for verification should succeed");
      }

      File downloadedFile2 = new File(verifyDir2.toFile(), fileName);
      assertTrue(downloadedFile2.exists(), "2nd downloaded file should exist");
      long downloadedSize2 = downloadedFile2.length();
      String downloadDigest2 = sha256Hex(downloadedFile2);
      System.out.println(
          "[LobSizeTest Diagnostic] Downloaded file (2nd GET): size="
              + downloadedSize2
              + " SHA-256="
              + downloadDigest2);

      if (!downloadDigest1.equals(downloadDigest2)) {
        String msg =
            "Two GETs produced different SHA-256: " + downloadDigest1 + " vs " + downloadDigest2;
        System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
        verificationErrors.append(msg).append("\n");
      } else {
        System.out.println(
            "[LobSizeTest Diagnostic] Two GETs are byte-identical. SHA-256 match confirmed.");
      }

      boolean clientSideCorruptionDetected = verificationErrors.length() > 0;
      if (clientSideCorruptionDetected) {
        System.out.println(
            "[LobSizeTest Diagnostic] === CLIENT-SIDE CORRUPTION DETECTED ===\n"
                + verificationErrors
                + "Proceeding to COPY INTO to check if server also rejects the file...");
      }

      // Clean up temp files
      downloadedFile.delete();
      verifyDir.toFile().delete();
      downloadedFile2.delete();
      verifyDir2.toFile().delete();

      // --- Proceed with COPY INTO regardless of verification outcome ---
      String copyInto =
          "copy into "
              + tableName
              + " from @%"
              + tableName
              + " file_format=(type=csv compression='none')";
      boolean copyIntoFailed = false;
      String copyIntoError = null;
      try {
        stmt.execute(copyInto);
      } catch (SQLException e) {
        copyIntoFailed = true;
        copyIntoError = e.getMessage();
        System.out.println(
            "[LobSizeTest Diagnostic] COPY INTO failed: "
                + e.getErrorCode()
                + " / "
                + e.getSQLState()
                + " / "
                + e.getMessage());
      }

      if (clientSideCorruptionDetected && copyIntoFailed) {
        fail(
            "Both client-side verification and server-side COPY INTO failed.\n"
                + "Client-side errors:\n"
                + verificationErrors
                + "COPY INTO error: "
                + copyIntoError
                + "\nThis confirms the JDBC driver uploaded corrupt data.");
      } else if (clientSideCorruptionDetected) {
        fail(
            "Client-side verification detected corruption, but COPY INTO succeeded.\n"
                + "Client-side errors:\n"
                + verificationErrors
                + "This is unexpected -- the server accepted data that failed local checks.");
      } else if (copyIntoFailed) {
        fail(
            "Client-side verification passed, but COPY INTO failed.\n"
                + "COPY INTO error: "
                + copyIntoError
                + "\nThis suggests corruption in the server pipeline,"
                + " not in the JDBC driver.");
      }

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
        assertEquals(fileName, rsGet.getString(1));
        assertEquals("DOWNLOADED", rsGet.getString(3));
      }
    }
  }

  /**
   * Compute SHA-256 hex digest of a file. Used only for diagnostic logging in tests -- does not
   * touch any production stream or upload logic.
   */
  private static String sha256Hex(File file) {
    try (FileInputStream fis = new FileInputStream(file)) {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] buf = new byte[8192];
      int n;
      while ((n = fis.read(buf)) != -1) {
        md.update(buf, 0, n);
      }
      byte[] digest = md.digest();
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (IOException | NoSuchAlgorithmException e) {
      return "ERROR: " + e.getMessage();
    }
  }
}
