package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for MinicoreTelemetry.
 *
 * <p>These tests verify telemetry data generation from load results, including ISA detection,
 * version information, error reporting, and log aggregation.
 */
public class MinicoreTelemetryTest {

  @Test
  public void testTelemetryFromSuccessfulLoad() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toMap();

    assertNotNull(telemetry, "Telemetry should not be null");

    // ISA should always be present
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertNotNull(map.get("ISA"), "ISA should not be null");
    assertTrue(
        map.get("ISA").toString().matches("amd64|arm64|ppc64|x86|Unknown"),
        "ISA should be a valid architecture: " + map.get("ISA"));

    if (result.isSuccess()) {
      // On success: should have version, should NOT have error
      assertTrue(map.containsKey("CORE_VERSION"), "Telemetry should contain CORE_VERSION");
      assertNotNull(map.get("CORE_VERSION"), "CORE_VERSION should not be null");
      assertTrue(
          map.get("CORE_VERSION").toString().contains("0.0.1"),
          "CORE_VERSION should contain version number");

      assertFalse(
          map.containsKey("CORE_LOAD_ERROR"),
          "Telemetry should not contain CORE_LOAD_ERROR on success");
    } else {
      // On failure: should have error, should NOT have version
      assertTrue(
          map.containsKey("CORE_LOAD_ERROR"),
          "Telemetry should contain CORE_LOAD_ERROR on failure");
      assertNotNull(map.get("CORE_LOAD_ERROR"), "CORE_LOAD_ERROR should not be null");
      assertFalse(
          map.containsKey("CORE_VERSION"), "Telemetry should not contain CORE_VERSION on failure");
    }

    // Logs should always be present
    assertTrue(map.containsKey("CORE_LOAD_LOGS"), "Telemetry should contain CORE_LOAD_LOGS");
    assertNotNull(map.get("CORE_LOAD_LOGS"), "CORE_LOAD_LOGS should not be null");
    assertTrue(
        map.get("CORE_LOAD_LOGS") instanceof java.util.List, "CORE_LOAD_LOGS should be a List");
  }

  @Test
  public void testTelemetryDoesNotContainOSOrOSVersion() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toMap();

    assertFalse(map.containsKey("OS"), "Telemetry should not contain OS (already in SessionUtil)");
    assertFalse(
        map.containsKey("OS_VERSION"),
        "Telemetry should not contain OS_VERSION (already in SessionUtil)");
  }

  @Test
  public void testTelemetryFromNullLoadResult() {
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(null);
    Map<String, Object> map = telemetry.toMap();

    assertNotNull(telemetry, "Telemetry should not be null even with null load result");
    assertTrue(
        map.containsKey("CORE_LOAD_ERROR"),
        "Telemetry should contain error when load result is null");
    assertEquals(
        "MinicoreManager not initialized",
        map.get("CORE_LOAD_ERROR"),
        "Error message should indicate not initialized");
  }

  @Test
  public void testTelemetryLogsAreTimestamped() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toMap();

    @SuppressWarnings("unchecked")
    java.util.List<String> logs = (java.util.List<String>) map.get("CORE_LOAD_LOGS");

    assertNotNull(logs, "Logs should not be null");
    assertFalse(logs.isEmpty(), "Logs should not be empty");

    for (String log : logs) {
      assertTrue(
          log.matches("^\\[\\d+\\.\\d{6}ms\\] .*"), "Each log should be timestamped: " + log);
    }
  }

  @Test
  public void testTelemetryToStringDoesNotThrow() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);

    assertDoesNotThrow(() -> telemetry.toString(), "toString should not throw");
    assertNotNull(telemetry.toString(), "toString should not return null");
  }

  @Test
  public void testTelemetryContainsCoreFileName() {
    MinicorePlatform platform = new MinicorePlatform();
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toMap();

    // CORE_FILE_NAME should be present and match platform
    assertTrue(map.containsKey("CORE_FILE_NAME"), "Telemetry should contain CORE_FILE_NAME");
    assertNotNull(map.get("CORE_FILE_NAME"), "CORE_FILE_NAME should not be null");

    String fileName = map.get("CORE_FILE_NAME").toString();
    assertTrue(
        fileName.contains("sf_mini_core"),
        "CORE_FILE_NAME should contain library base name: " + fileName);

    String expectedFileName = platform.getLibraryFileName();
    assertEquals(
        expectedFileName, fileName, "CORE_FILE_NAME should match platform's library file name");
  }
}
