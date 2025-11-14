package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * IPv6 Connectivity Test for Snowflake JDBC Driver
 *
 * <p>This test verifies IPv6 connectivity with Snowflake by performing: 1. SELECT 1 - Basic query
 * execution 2. SELECT pi() - Function execution 3. PUT operation - File upload to internal stage 4.
 * GET operation - File download from internal stage
 *
 * <p>The test uses driver-wide parameters for authentication (supports both password and private
 * key authentication).
 *
 * <p>Environment variables required: - SNOWFLAKE_TEST_HOST - SNOWFLAKE_TEST_ACCOUNT -
 * SNOWFLAKE_TEST_USER - SNOWFLAKE_TEST_PASSWORD or SNOWFLAKE_TEST_PRIVATE_KEY_FILE -
 * SNOWFLAKE_TEST_DATABASE - SNOWFLAKE_TEST_SCHEMA - SNOWFLAKE_TEST_WAREHOUSE -
 * SNOWFLAKE_TEST_ROLE
 */
public class IPv6ConnectivityIT extends BaseJDBCTest {
  
  // Static initializer block - runs BEFORE anything else, including logger initialization
  static {
    // Disable AWS/Azure/GCP metadata service checks to avoid hanging on local machine
    System.setProperty("com.amazonaws.sdk.disableEc2Metadata", "true");
    System.setProperty("aws.disableEc2Metadata", "true");
    System.setProperty("AWS_EC2_METADATA_DISABLED", "true");
    // Disable deprecation warnings
    System.setProperty("aws.java.v1.disableDeprecationAnnouncement", "true");
    
    // CRITICAL: Initialize platform detection cache with empty results BEFORE any connection attempt
    // This prevents the default platform detection from running with 200ms timeout
    try {
      java.lang.reflect.Field cacheField = 
          net.snowflake.client.util.PlatformDetector.class.getDeclaredField("cachedDetectedPlatforms");
      cacheField.setAccessible(true);
      cacheField.set(null, java.util.Collections.emptyList());
    } catch (Exception e) {
      // If reflection fails, platform detection will still run, but at least we tried
      System.err.println("WARNING: Failed to pre-initialize platform detection cache: " + e.getMessage());
    }
  }
  
  private static final SFLogger logger = SFLoggerFactory.getLogger(IPv6ConnectivityIT.class);
  private static final String TEST_FILE_PREFIX = "ipv6_test_";
  private static final int TEST_FILE_SIZE_KB = 10;
  private static Path tempTestFile;

  @BeforeAll
  public static void setUp() throws IOException {
    logger.info("========================================");
    logger.info("IPv6 Connectivity Test - Setup");
    logger.info("========================================");
    logger.info("AWS metadata checks disabled: {}", 
        System.getProperty("com.amazonaws.sdk.disableEc2Metadata"));
  }

  @AfterAll
  public static void tearDown() {
    logger.info("========================================");
    logger.info("IPv6 Connectivity Test - Teardown");
    logger.info("========================================");
    if (tempTestFile != null && Files.exists(tempTestFile)) {
      try {
        Files.delete(tempTestFile);
        logger.info("Cleaned up temporary test file: {}", tempTestFile);
      } catch (IOException e) {
        logger.warn("Failed to delete temporary test file: {}", e.getMessage());
      }
    }
  }

  /**
   * Get a connection with platform detection disabled to avoid metadata service timeouts
   */
  private Connection getConnectionWithoutPlatformDetection() throws SQLException {
    logger.info("[DEBUG] Creating connection properties with platform detection disabled...");
    Properties props = new Properties();
    
    // Disable platform detection completely
    props.put("disablePlatformDetection", "true");
    
    // Also set timeout to 0 as a fallback (skips network checks)
    props.put("platformDetectionTimeoutMs", "0");
    
    logger.info("[DEBUG] Properties set: disablePlatformDetection=true, platformDetectionTimeoutMs=0");
    
    return getConnection(props);
  }

  @Test
  public void testIPv6Connectivity() throws SQLException, IOException {
    logger.info("========================================");
    logger.info("Starting IPv6 Connectivity Test");
    logger.info("========================================");
    
    // Get connection parameters for network debugging
    Map<String, String> params = getConnectionParameters();
    String hostname = params.get("host");
    
    // Network debugging: DNS resolution
    logger.info("[NETWORK] Resolving DNS for host: {}", hostname);
    try {
      InetAddress[] addresses = InetAddress.getAllByName(hostname);
      logger.info("[NETWORK] DNS resolution found {} address(es):", addresses.length);
      for (InetAddress addr : addresses) {
        if (addr instanceof Inet6Address) {
          logger.info("[NETWORK]   - IPv6: {} ({})", addr.getHostAddress(), addr.getClass().getSimpleName());
        } else if (addr instanceof Inet4Address) {
          logger.info("[NETWORK]   - IPv4: {} ({})", addr.getHostAddress(), addr.getClass().getSimpleName());
        } else {
          logger.info("[NETWORK]   - Unknown: {} ({})", addr.getHostAddress(), addr.getClass().getSimpleName());
        }
      }
    } catch (UnknownHostException e) {
      logger.error("[NETWORK] DNS resolution failed: {}", e.getMessage());
      throw e;
    }
    
    logger.info("[DEBUG] Step 1: Attempting to connect to Snowflake...");
    long startTime = System.currentTimeMillis();
    
    try (Connection connection = getConnectionWithoutPlatformDetection()) {
      long connectionTime = System.currentTimeMillis() - startTime;
      logger.info("[DEBUG] Step 1: DONE - Connected successfully in {} ms", connectionTime);
      logger.info("[NETWORK] Connection established to: {}", hostname);

      logger.info("[DEBUG] Step 2: Creating statement...");
      try (Statement statement = connection.createStatement()) {
        logger.info("[DEBUG] Step 2: DONE - Statement created");
        
        // Test 1: SELECT 1
        logger.info("[DEBUG] Step 3: Running Test 1 (SELECT 1)...");
        testSelect1(statement);
        logger.info("[DEBUG] Step 3: DONE");

        // Test 2: SELECT pi()
        logger.info("[DEBUG] Step 4: Running Test 2 (SELECT pi())...");
        testSelectPi(statement);
        logger.info("[DEBUG] Step 4: DONE");

        // Test 3 & 4: PUT and GET operations
        logger.info("[DEBUG] Step 5: Running Tests 3 & 4 (PUT/GET)...");
        testPutAndGet(statement);
        logger.info("[DEBUG] Step 5: DONE");
      }
    }

    logger.info("========================================");
    logger.info("IPv6 Connectivity Test Completed Successfully");
    logger.info("========================================");
  }

  /**
   * Test 1: Execute SELECT 1
   *
   * @param statement SQL statement
   * @throws SQLException if query fails
   */
  private void testSelect1(Statement statement) throws SQLException {
    logger.info("========================================");
    logger.info("Test 1: Executing SELECT 1");
    logger.info("========================================");

    try (ResultSet rs = statement.executeQuery("SELECT 1")) {
      assertTrue(rs.next(), "ResultSet should have at least one row");
      int result = rs.getInt(1);
      logger.info("SELECT 1 result: {}", result);
      assertEquals(1, result, "Expected 1, got " + result);
      logger.info("✓ Test 1 PASSED: SELECT 1 executed successfully");
    }
  }

  /**
   * Test 2: Execute SELECT pi()
   *
   * @param statement SQL statement
   * @throws SQLException if query fails
   */
  private void testSelectPi(Statement statement) throws SQLException {
    logger.info("========================================");
    logger.info("Test 2: Executing SELECT pi()");
    logger.info("========================================");

    try (ResultSet rs = statement.executeQuery("SELECT pi()")) {
      assertTrue(rs.next(), "ResultSet should have at least one row");
      double piValue = rs.getDouble(1);
      logger.info("SELECT pi() result: {}", piValue);

      double expectedPi = Math.PI;
      double delta = 0.000001;
      assertTrue(
          Math.abs(piValue - expectedPi) < delta,
          String.format("Expected pi (%.15f), got %.15f", expectedPi, piValue));
      logger.info("✓ Test 2 PASSED: SELECT pi() executed successfully");
    }
  }

  /**
   * Test 3 & 4: Execute PUT and GET operations
   *
   * @param statement SQL statement
   * @throws SQLException if operations fail
   * @throws IOException if file operations fail
   */
  private void testPutAndGet(Statement statement) throws SQLException, IOException {
    logger.info("========================================");
    logger.info("Test 3 & 4: Starting PUT and GET operations");
    logger.info("========================================");

    // Create temporary directory and file
    Path tempDir = Files.createTempDirectory("ipv6_test_");
    String fileName = TEST_FILE_PREFIX + System.currentTimeMillis() + ".txt";
    tempTestFile = tempDir.resolve(fileName);

    // Generate random test file
    logger.info("Generating test file: {}", tempTestFile);
    generateRandomFile(tempTestFile, TEST_FILE_SIZE_KB);
    long fileSize = Files.size(tempTestFile);
    logger.info("Test file size: {} bytes", fileSize);
    assertTrue(fileSize > 0, "Test file should not be empty");

    // Use user stage (internal stage, no cloud credentials needed)
    String stageName = "~"; // User stage
    logger.info("Using user stage: {}", stageName);

    try {
      // Test 3: PUT file to stage
      testPutOperation(statement, tempTestFile, stageName);

      // Test 4: GET file from stage
      testGetOperation(statement, tempDir, stageName, fileName);

      // Clean up: remove file from stage
      cleanupStage(statement, stageName, fileName);
    } finally {
      // Clean up temporary directory
      try {
        Files.deleteIfExists(tempTestFile);
        Files.deleteIfExists(tempDir);
        logger.info("Cleaned up temporary directory: {}", tempDir);
      } catch (IOException e) {
        logger.warn("Failed to clean up temporary directory: {}", e.getMessage());
      }
    }
  }

  /**
   * Test 3: PUT operation - upload file to stage
   *
   * @param statement SQL statement
   * @param testFile file to upload
   * @param stageName stage name
   * @throws SQLException if PUT fails
   */
  private void testPutOperation(Statement statement, Path testFile, String stageName)
      throws SQLException {
    logger.info("========================================");
    logger.info("Test 3: PUT Operation");
    logger.info("========================================");

    String putSql = String.format("PUT file://%s @%s", testFile.toString(), stageName);
    logger.info("Executing PUT: {}", putSql);

    try (ResultSet rs = statement.executeQuery(putSql)) {
      assertTrue(rs.next(), "PUT should return results");

      // PUT result columns: source, target, source_size, target_size, source_compression,
      // target_compression, status, message
      String source = rs.getString(1);
      String status = rs.getString(7);

      logger.info("PUT result:");
      logger.info("  Source: {}", source);
      logger.info("  Status: {}", status);

      assertNotNull(status, "PUT status should not be null");
      assertTrue(
          status.equals("UPLOADED") || status.equals("SKIPPED"),
          "File should be uploaded or skipped, got status: " + status);

      logger.info("✓ Test 3 PASSED: PUT operation completed successfully");
    }
  }

  /**
   * Test 4: GET operation - download file from stage
   *
   * @param statement SQL statement
   * @param outputDir output directory
   * @param stageName stage name
   * @param fileName file name
   * @throws SQLException if GET fails
   * @throws IOException if file operations fail
   */
  private void testGetOperation(
      Statement statement, Path outputDir, String stageName, String fileName)
      throws SQLException, IOException {
    logger.info("========================================");
    logger.info("Test 4: GET Operation");
    logger.info("========================================");

    // List files in stage first
    String listSql = String.format("LIST @%s", stageName);
    logger.info("Listing files in stage to verify upload...");

    boolean fileFound = false;
    int totalFiles = 0;
    try (ResultSet rs = statement.executeQuery(listSql)) {
      while (rs.next()) {
        String stagedFile = rs.getString(1);
        totalFiles++;
        if (stagedFile.contains(fileName)) {
          fileFound = true;
          logger.info("✓ Found uploaded file: {}", stagedFile);
        }
      }
    }
    
    logger.info("Stage contains {} file(s) total", totalFiles);
    assertTrue(fileFound, "Uploaded file should be found in stage listing");

    // Create download directory
    Path downloadDir = outputDir.resolve("download");
    Files.createDirectories(downloadDir);

    // GET file from stage (file is automatically gzipped by PUT)
    String getSql =
        String.format("GET @%s/%s.gz file://%s/", stageName, fileName, downloadDir.toString());
    logger.info("Executing GET: {}", getSql);

    try (ResultSet rs = statement.executeQuery(getSql)) {
      assertTrue(rs.next(), "GET should return results");

      String file = rs.getString(1);
      long size = rs.getLong(2);
      String status = rs.getString(3);

      logger.info("GET result:");
      logger.info("  File: {}", file);
      logger.info("  Size: {} bytes", size);
      logger.info("  Status: {}", status);

      assertEquals("DOWNLOADED", status, "File should be downloaded");
      assertTrue(size > 0, "Downloaded file should not be empty");

      logger.info("✓ Test 4 PASSED: GET operation completed successfully");
    }

    // Verify downloaded file exists
    File[] downloadedFiles = downloadDir.toFile().listFiles((dir, name) -> name.endsWith(".gz"));
    assertNotNull(downloadedFiles, "Downloaded files array should not be null");
    assertTrue(downloadedFiles.length > 0, "At least one file should be downloaded");
    logger.info("Downloaded {} file(s)", downloadedFiles.length);

    for (File file : downloadedFiles) {
      logger.info("  Downloaded file: {} ({} bytes)", file.getName(), file.length());
    }
  }

  /**
   * Clean up: remove file from stage
   *
   * @param statement SQL statement
   * @param stageName stage name
   * @param fileName file name
   */
  private void cleanupStage(Statement statement, String stageName, String fileName) {
    logger.info("Cleaning up: removing file from stage");
    try {
      String removeSql = String.format("REMOVE @%s/%s.gz", stageName, fileName);
      logger.info("Executing: {}", removeSql);
      statement.execute(removeSql);
      logger.info("✓ Cleanup completed");
    } catch (SQLException e) {
      logger.warn("Failed to remove file from stage: {}", e.getMessage());
    }
  }

  /**
   * Generate a random text file
   *
   * @param filePath path to create file
   * @param sizeKB size in kilobytes
   * @throws IOException if file creation fails
   */
  private void generateRandomFile(Path filePath, int sizeKB) throws IOException {
    Random random = new Random();
    StringBuilder content = new StringBuilder();

    // Generate random alphanumeric content
    String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\n";
    int targetSize = sizeKB * 1024;

    while (content.length() < targetSize) {
      content.append(chars.charAt(random.nextInt(chars.length())));
    }

    try (FileWriter writer = new FileWriter(filePath.toFile())) {
      writer.write(content.toString());
    }

    logger.debug("Generated random file: {} ({} bytes)", filePath, Files.size(filePath));
  }
}

