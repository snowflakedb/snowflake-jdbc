package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Tests for MinicoreLoader.
 *
 * <p>These tests verify the minicore native library loading functionality, including platform
 * detection, resource extraction, and version retrieval.
 */
public class MinicoreLoaderTest {

  @Test
  public void testLoadLibraryReturnsResult() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    assertNotNull(result, "Load result should not be null");
  }

  @Test
  public void testLoadLibraryIsCached() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result1 = loader.loadLibrary();
    MinicoreLoadResult result2 = loader.loadLibrary();

    assertSame(result1, result2, "Subsequent calls should return the same cached result");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testLoadLibrarySuccessOnSupportedPlatform() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (!result.isSuccess()) {
      System.out.println("=== MINICORE LOAD FAILED ===");
      System.out.println("Error: " + result.getErrorMessage());
      if (result.getException() != null) {
        System.out.println("Exception: " + result.getException().getMessage());
        result.getException().printStackTrace(System.out);
      }
      System.out.println("Load Logs:");
      for (String log : result.getLogs()) {
        System.out.println("  " + log);
      }
      System.out.println("=== END DEBUG ===");
    }

    assertTrue(result.isSuccess(), "Loading should succeed on supported platforms");
    assertNull(result.getErrorMessage(), "Error message should be null on success");
    assertNull(result.getException(), "Exception should be null on success");
    assertNotNull(loader.getLibrary(), "Library instance should be available after successful load");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testGetVersionAfterSuccessfulLoad() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      MinicoreLibrary library = loader.getLibrary();
      assertNotNull(library, "Library should not be null after successful load");

      String version = library.sf_core_full_version();
      assertNotNull(version, "Version should not be null");
      assertFalse(version.isEmpty(), "Version should not be empty");
      assertTrue(version.contains("0.0.1"), "Version should contain expected version string");
    }
  }

  @Test
  public void testLoadResultContainsLogs() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    assertNotNull(result.getLogs(), "Logs should not be null");
    assertFalse(result.getLogs().isEmpty(), "Logs should contain entries");

    // Verify first log entry
    String firstLog = result.getLogs().get(0);
    assertTrue(firstLog.contains("Starting minicore loading"), "First log should indicate start");
    assertTrue(firstLog.matches("\\[\\d+\\.\\d+ms\\].*"), "Log should have timestamp format");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testLogsContainKeySteps() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    String allLogs = String.join("\n", result.getLogs());

    assertTrue(allLogs.contains("Starting minicore loading"), "Should log start");
    assertTrue(allLogs.contains("Detected platform"), "Should log platform detection");

    if (result.isSuccess()) {
      assertTrue(allLogs.contains("Platform supported"), "Should log platform support");
      assertTrue(allLogs.contains("Library resource path"), "Should log resource path");
      assertTrue(allLogs.contains("Created temp directory") || allLogs.contains("Using working directory") || allLogs.contains("Using home directory"), 
          "Should log directory selection");
      assertTrue(allLogs.contains("Writing embedded library"), "Should log library extraction");
      assertTrue(allLogs.contains("Successfully wrote embedded library"), "Should log successful write");
      assertTrue(allLogs.contains("Setting file permissions"), "Should log permission setting");
      assertTrue(allLogs.contains("Calling Native.load"), "Should log native load call");
      assertTrue(allLogs.contains("Native.load finished"), "Should log native load completion");
      assertTrue(allLogs.contains("Minicore library loaded successfully"), "Should log success");
      // Note: Cleanup happens in finally block after result is returned, so it won't be in logs
      // assertTrue(allLogs.contains("Deleted temporary library file"), "Should log cleanup");
    }
  }

  @Test
  public void testLoadResultLogsAreTimestamped() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    for (String log : result.getLogs()) {
      assertTrue(log.matches("^\\[\\d+\\.\\d{6}ms\\] .*"), 
          "Each log entry should start with timestamp in format [123.456789ms]: " + log);
    }
  }

  @Test
  public void testTimestampsAreMonotonicallyIncreasing() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    double previousTime = -1.0;
    for (String log : result.getLogs()) {
      // Extract timestamp from format "[123.456789ms] message"
      String timestampStr = log.substring(1, log.indexOf("ms]"));
      double timestamp = Double.parseDouble(timestampStr);

      assertTrue(timestamp >= previousTime, 
          "Timestamps should be monotonically increasing: " + timestamp + " should be >= " + previousTime);
      previousTime = timestamp;
    }
  }
}
