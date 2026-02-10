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
import java.util.zip.GZIPInputStream;
import net.snowflake.client.annotations.DontRunOnJenkins;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.ObjectMapperFactory;
import net.snowflake.client.internal.core.UUIDUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
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
        // ResultSet should return a row with the zipped file name
        assertTrue(rsFiles.next());
        assertEquals(fileName + ".gz", rsFiles.getString(1));
        stagedFileSize = rsFiles.getLong(2);
        System.out.println(
            "[LobSizeTest Diagnostic] Staged file: "
                + rsFiles.getString(1)
                + " stagedSize="
                + stagedFileSize
                + " bytes");
      }
      assertTrue(
          stagedFileSize > 0,
          "Staged .gz file size should be positive but was " + stagedFileSize);

      // --- Non-fatal GZIP integrity verification ---
      // All checks below are collected as warnings so the test always proceeds to COPY INTO.
      // This lets us correlate client-side corruption with server-side decompression errors.
      StringBuilder verificationErrors = new StringBuilder();

      // Download the .gz file from the stage
      Path verifyDir = Files.createTempDirectory("LobSizeVerify");
      verifyDir.toFile().deleteOnExit();
      String verifyDirEscaped = verifyDir.toString().replace("\\", "\\\\");

      stmt.execute("get @%" + tableName + " 'file://" + verifyDirEscaped + "'");
      try (ResultSet rsVerify = stmt.getResultSet()) {
        assertTrue(rsVerify.next(), "GET for GZIP verification should return a result");
        assertEquals(
            "DOWNLOADED", rsVerify.getString(3), "GET for GZIP verification should succeed");
      }

      File downloadedGzFile = new File(verifyDir.toFile(), fileName + ".gz");
      assertTrue(
          downloadedGzFile.exists(),
          "Downloaded .gz file should exist at: " + downloadedGzFile.getAbsolutePath());
      long downloadedGzSize = downloadedGzFile.length();

      // Check the size gap between LS (encrypted) and GET (decrypted) for AES-CBC padding
      long sizeDiff = stagedFileSize - downloadedGzSize;
      long expectedPadding = 16 - (downloadedGzSize % 16);
      System.out.println(
          "[LobSizeTest Diagnostic] Downloaded .gz size="
              + downloadedGzSize
              + " stagedSize="
              + stagedFileSize
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

      if (downloadedGzSize <= 0) {
        String msg = "Downloaded .gz file is empty (size=0)";
        System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
        verificationErrors.append(msg).append("\n");
      }

      // Read the first bytes to check GZIP magic header (1f 8b)
      try (FileInputStream headerCheck = new FileInputStream(downloadedGzFile)) {
        int b1 = headerCheck.read();
        int b2 = headerCheck.read();
        System.out.println(
            "[LobSizeTest Diagnostic] .gz magic bytes: "
                + String.format("0x%02x 0x%02x", b1, b2)
                + (b1 == 0x1f && b2 == 0x8b ? " (valid GZIP)" : " (NOT GZIP!)"));
        if (b1 != 0x1f || b2 != 0x8b) {
          // Dump more header bytes for diagnosis
          byte[] head = new byte[32];
          head[0] = (byte) b1;
          head[1] = (byte) b2;
          int extra = headerCheck.read(head, 2, head.length - 2);
          StringBuilder hexDump = new StringBuilder();
          for (int i = 0; i < 2 + Math.max(0, extra); i++) {
            hexDump.append(String.format("%02x ", head[i] & 0xff));
          }
          String msg =
              "NOT GZIP: first bytes are ["
                  + hexDump.toString().trim()
                  + "]. Expected 1f 8b. This means the uploaded data is not"
                  + " valid GZIP -- corruption occurred during compress/encrypt/upload.";
          System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
          verificationErrors.append(msg).append("\n");
        }
      }

      // Compute SHA-256 of the .gz file for traceability
      String gzDigest1 = sha256Hex(downloadedGzFile);
      System.out.println("[LobSizeTest Diagnostic] .gz SHA-256 (1st GET): " + gzDigest1);

      // Attempt full decompression to verify GZIP stream integrity
      long decompressedSize = 0;
      try (FileInputStream fis = new FileInputStream(downloadedGzFile);
          GZIPInputStream gzis = new GZIPInputStream(fis)) {
        byte[] buf = new byte[8192];
        int bytesRead;
        while ((bytesRead = gzis.read(buf)) != -1) {
          decompressedSize += bytesRead;
        }
        System.out.println(
            "[LobSizeTest Diagnostic] GZIP decompression PASSED. decompressedSize="
                + decompressedSize
                + " originalSize="
                + originalFileSize);
        if (decompressedSize != originalFileSize) {
          String msg =
              "Decompressed size mismatch: decompressed="
                  + decompressedSize
                  + " original="
                  + originalFileSize;
          System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
          verificationErrors.append(msg).append("\n");
        }
      } catch (IOException e) {
        String msg =
            "GZIP decompression FAILED: "
                + e.getMessage()
                + " | downloadedGzSize="
                + downloadedGzSize
                + " stagedSize="
                + stagedFileSize
                + " originalSize="
                + originalFileSize
                + " gzSHA256="
                + gzDigest1;
        System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
        verificationErrors.append(msg).append("\n");
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

      File downloadedGzFile2 = new File(verifyDir2.toFile(), fileName + ".gz");
      assertTrue(downloadedGzFile2.exists(), "2nd downloaded .gz file should exist");
      long downloadedGzSize2 = downloadedGzFile2.length();
      String gzDigest2 = sha256Hex(downloadedGzFile2);
      System.out.println(
          "[LobSizeTest Diagnostic] .gz SHA-256 (2nd GET): "
              + gzDigest2
              + " size="
              + downloadedGzSize2);

      if (downloadedGzSize != downloadedGzSize2) {
        String msg =
            "Two GETs produced different sizes: "
                + downloadedGzSize
                + " vs "
                + downloadedGzSize2;
        System.out.println("[LobSizeTest Diagnostic] ERROR: " + msg);
        verificationErrors.append(msg).append("\n");
      }
      if (!gzDigest1.equals(gzDigest2)) {
        String msg =
            "Two GETs produced different SHA-256: " + gzDigest1 + " vs " + gzDigest2;
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
      downloadedGzFile.delete();
      verifyDir.toFile().delete();
      downloadedGzFile2.delete();
      verifyDir2.toFile().delete();

      // --- Proceed with COPY INTO regardless of verification outcome ---
      String copyInto =
          "copy into "
              + tableName
              + " from @%"
              + tableName
              + " file_format=(type=csv compression='gzip')";
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
            "Both client-side GZIP verification and server-side COPY INTO failed.\n"
                + "Client-side errors:\n"
                + verificationErrors
                + "COPY INTO error: "
                + copyIntoError
                + "\nThis confirms the JDBC driver uploaded corrupt data.");
      } else if (clientSideCorruptionDetected) {
        fail(
            "Client-side GZIP verification detected corruption, but COPY INTO succeeded.\n"
                + "Client-side errors:\n"
                + verificationErrors
                + "This is unexpected -- the server accepted data that failed local checks.");
      } else if (copyIntoFailed) {
        fail(
            "Client-side GZIP verification passed, but COPY INTO failed.\n"
                + "COPY INTO error: "
                + copyIntoError
                + "\nThis suggests corruption occurred in the server's decrypt/decompress"
                + " pipeline, not in the JDBC driver.");
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
        assertEquals(fileName + ".gz", rsGet.getString(1));
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
