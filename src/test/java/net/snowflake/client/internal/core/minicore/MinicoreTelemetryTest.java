package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for MinicoreTelemetry.
 *
 * <p>These tests verify telemetry data generation from load results, including ISA detection,
 * version information, error reporting, and log aggregation. Uses mock MinicoreLoadResult objects
 * to test telemetry generation in isolation.
 */
public class MinicoreTelemetryTest {

  private static final String TEST_LIBRARY_FILE = "libsf_mini_core_test.so";
  private static final String TEST_VERSION = "0.0.1-test";
  private static final List<String> TEST_LOGS =
      Arrays.asList("[0.001ms] Log entry 1", "[0.002ms] Log entry 2");

  @Test
  public void testTelemetryFromSuccessfulLoad() {
    MinicoreLoadResult result =
        MinicoreLoadResult.success(TEST_LIBRARY_FILE, null, TEST_VERSION, TEST_LOGS);
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toMap();

    assertNotNull(telemetry, "Telemetry should not be null");
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertNotNull(map.get("ISA"), "ISA should not be null");
    assertTrue(map.containsKey("CORE_VERSION"), "Telemetry should contain CORE_VERSION");
    assertEquals(TEST_VERSION, map.get("CORE_VERSION"), "CORE_VERSION should match");
    assertTrue(map.containsKey("CORE_FILE_NAME"), "Telemetry should contain CORE_FILE_NAME");
    assertEquals(TEST_LIBRARY_FILE, map.get("CORE_FILE_NAME"), "CORE_FILE_NAME should match");
    assertFalse(
        map.containsKey("CORE_LOAD_ERROR"), "Should not contain CORE_LOAD_ERROR on success");
    assertTrue(map.containsKey("CORE_LOAD_LOGS"), "Telemetry should contain CORE_LOAD_LOGS");
    assertEquals(TEST_LOGS, map.get("CORE_LOAD_LOGS"), "CORE_LOAD_LOGS should match");
  }

  @Test
  public void testTelemetryFromFailedLoad() {
    String errorMessage = "Failed to load library: symbol not found";
    MinicoreLoadResult result =
        MinicoreLoadResult.failure(
            errorMessage, TEST_LIBRARY_FILE, new UnsatisfiedLinkError("test"), TEST_LOGS);

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toMap();
    assertNotNull(telemetry, "Telemetry should not be null");
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertTrue(map.containsKey("CORE_LOAD_ERROR"), "Should contain CORE_LOAD_ERROR on failure");
    assertEquals(errorMessage, map.get("CORE_LOAD_ERROR"), "CORE_LOAD_ERROR should match");
    assertTrue(map.containsKey("CORE_FILE_NAME"), "Telemetry should contain CORE_FILE_NAME");
    assertEquals(TEST_LIBRARY_FILE, map.get("CORE_FILE_NAME"), "CORE_FILE_NAME should match");
    assertFalse(map.containsKey("CORE_VERSION"), "Should not contain CORE_VERSION on failure");
    assertTrue(map.containsKey("CORE_LOAD_LOGS"), "Telemetry should contain CORE_LOAD_LOGS");
  }

  @Test
  public void testTelemetryFromNullLoadResult() {
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(null);
    Map<String, Object> map = telemetry.toMap();

    assertNotNull(telemetry, "Telemetry should not be null even with null load result");
    assertTrue(map.containsKey("CORE_LOAD_ERROR"), "Should contain error when load result is null");
    assertEquals(
        "MinicoreManager not initialized",
        map.get("CORE_LOAD_ERROR"),
        "Error message should indicate not initialized");
  }
}
