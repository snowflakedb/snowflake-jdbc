package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

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
  public void testLoadResultContainsAllFields() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();
    MinicorePlatform platform = new MinicorePlatform();

    // Library file name should always be present
    assertNotNull(result.getLibraryFileName(), "Library file name should not be null");
    String expectedFileName = platform.getLibraryFileName();
    assertTrue(
        result.getLibraryFileName().contains("sf_mini_core"),
        "Library file name should contain base name");
    assertTrue(
        result.getLibraryFileName().equals(expectedFileName),
        "Library file name should match platform's expected file name");

    // Logs should always be present and non-empty
    assertNotNull(result.getLogs(), "Logs should not be null");
    assertFalse(result.getLogs().isEmpty(), "Logs should contain entries");
    String firstLog = result.getLogs().get(0);
    assertTrue(firstLog.contains("Starting minicore loading"), "First log should indicate start");
    assertTrue(firstLog.matches("\\[\\d+\\.\\d+ms\\].*"), "Log should have timestamp format");

    if (result.isSuccess()) {
      // On success: should NOT have error message or exception
      assertNull(result.getErrorMessage(), "Error message should be null on success");
      assertNull(result.getException(), "Exception should be null on success");
    } else {
      // On failure: should have error message
      assertNotNull(result.getErrorMessage(), "Error message should be set on failure");
      // Exception may or may not be present depending on failure type
    }
  }

  @Test
  public void testLogsContainKeySteps() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    String allLogs = String.join("\n", result.getLogs());

    assertTrue(allLogs.contains("Starting minicore loading"), "Should log start");
    assertTrue(allLogs.contains("Detected platform"), "Should log platform detection");

    MinicorePlatform platform = new MinicorePlatform();
    if (result.isSuccess() && platform.isSupported()) {
      assertTrue(allLogs.contains("Platform supported"), "Should log platform support");
      assertTrue(allLogs.contains("Library resource path"), "Should log resource path");
      assertTrue(
          allLogs.contains("Using temp directory")
              || allLogs.contains("Using working directory")
              || allLogs.contains("Using home directory"),
          "Should log directory selection");
      assertTrue(
          allLogs.contains("Writing embedded library to disk"), "Should log library extraction");
      assertTrue(
          allLogs.contains("Successfully wrote embedded library"), "Should log successful write");
      assertTrue(allLogs.contains("Setting file permissions"), "Should log permission setting");
      assertTrue(allLogs.contains("Calling Native.load"), "Should log native load call");
      assertTrue(allLogs.contains("Native.load finished"), "Should log native load completion");
      assertTrue(allLogs.contains("Minicore library loaded successfully"), "Should log success");
    }
  }

  @Test
  public void testLoadResultLogsAreTimestamped() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    for (String log : result.getLogs()) {
      assertTrue(
          log.matches("^\\[\\d+\\.\\d{6}ms\\] .*"),
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

      assertTrue(
          timestamp >= previousTime,
          "Timestamps should be monotonically increasing: "
              + timestamp
              + " should be >= "
              + previousTime);
      previousTime = timestamp;
    }
  }
}
