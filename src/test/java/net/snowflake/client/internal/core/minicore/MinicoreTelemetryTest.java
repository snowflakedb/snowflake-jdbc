package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
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

  @AfterEach
  public void tearDown() {
    Minicore.resetForTesting();
  }

  @Test
  public void testClientEnvTelemetryFromSuccessfulLoad() {
    MinicoreLoadResult result =
        MinicoreLoadResult.success(TEST_LIBRARY_FILE, null, TEST_VERSION, new ArrayList<>());
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toClientEnvironmentTelemetryMap();

    assertNotNull(telemetry, "Telemetry should not be null");
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertNotNull(map.get("ISA"), "ISA should not be null");
    assertTrue(map.containsKey("CORE_VERSION"), "Telemetry should contain CORE_VERSION");
    assertEquals(TEST_VERSION, map.get("CORE_VERSION"), "CORE_VERSION should match");
    assertTrue(map.containsKey("CORE_FILE_NAME"), "Telemetry should contain CORE_FILE_NAME");
    assertEquals(TEST_LIBRARY_FILE, map.get("CORE_FILE_NAME"), "CORE_FILE_NAME should match");
    assertFalse(
        map.containsKey("CORE_LOAD_ERROR"), "Should not contain CORE_LOAD_ERROR on success");
  }

  @Test
  public void testClientEnvTelemetryFromFailedLoad() {
    String errorMessage = "Failed to load library: symbol not found";
    MinicoreLoadResult result =
        MinicoreLoadResult.failure(
            errorMessage, TEST_LIBRARY_FILE, new UnsatisfiedLinkError("test"), new ArrayList<>());

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    Map<String, Object> map = telemetry.toClientEnvironmentTelemetryMap();

    assertNotNull(telemetry, "Telemetry should not be null");
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertTrue(map.containsKey("CORE_FILE_NAME"), "Telemetry should contain CORE_FILE_NAME");
    assertEquals(TEST_LIBRARY_FILE, map.get("CORE_FILE_NAME"), "CORE_FILE_NAME should match");
    assertFalse(map.containsKey("CORE_VERSION"), "Should not contain CORE_VERSION on failure");
    assertTrue(map.containsKey("CORE_LOAD_ERROR"), "Should contain CORE_LOAD_ERROR on failure");
    assertEquals(
        MinicoreLoadError.FAILED_TO_LOAD.getMessage(),
        map.get("CORE_LOAD_ERROR"),
        "CORE_LOAD_ERROR should match enum message");
  }

  @Test
  public void testTelemetryWhenStillLoading() {
    // Don't initialize minicore - simulates "still loading" state
    Minicore.resetForTesting();

    MinicoreTelemetry telemetry = MinicoreTelemetry.create();
    Map<String, Object> map = telemetry.toClientEnvironmentTelemetryMap();

    assertNotNull(telemetry, "Telemetry should not be null");
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertTrue(map.containsKey("CORE_LOAD_ERROR"), "Should contain CORE_LOAD_ERROR");
    assertEquals(
        MinicoreLoadError.STILL_LOADING.getMessage(),
        map.get("CORE_LOAD_ERROR"),
        "CORE_LOAD_ERROR should match enum message");

    // Check in-band telemetry has the correct error
    ObjectNode node = telemetry.toInBandTelemetryNode();
    assertEquals(MinicoreLoadError.STILL_LOADING.getMessage(), node.get("error").asText());
  }

  @Test
  public void testFailedLoadPreservesDetailedErrorInLogs() {
    String detailedError = "Library resource not found in JAR: /native/linux/libsf_mini_core.so";
    RuntimeException exception = new RuntimeException("File not found");
    List<String> originalLogs = Arrays.asList("Starting load", "Checking platform");

    MinicoreLoadResult result =
        MinicoreLoadResult.failure(detailedError, TEST_LIBRARY_FILE, exception, originalLogs);
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    ObjectNode node = telemetry.toInBandTelemetryNode();

    // Error field should have the enum message
    assertEquals(
        MinicoreLoadError.FAILED_TO_LOAD.getMessage(),
        node.get("error").asText(),
        "Error should be the enum message");

    // Logs should contain the detailed error and exception
    assertTrue(node.has("loadLogs"), "Should have loadLogs");
    List<String> logs = new ArrayList<>();
    node.get("loadLogs").forEach(n -> logs.add(n.asText()));

    assertTrue(
        logs.stream().anyMatch(log -> log.contains(detailedError)),
        "Logs should contain detailed error message");
    assertTrue(
        logs.stream().anyMatch(log -> log.contains("RuntimeException")),
        "Logs should contain exception class name");
  }

  // Tests for toInBandTelemetryNode()

  @Test
  public void testInBandTelemetryNodeFromSuccessfulLoad() {
    List<String> logs = Arrays.asList("log1", "log2", "log3");
    MinicoreLoadResult result =
        MinicoreLoadResult.success(TEST_LIBRARY_FILE, null, TEST_VERSION, logs);
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    ObjectNode node = telemetry.toInBandTelemetryNode();

    assertNotNull(node, "ObjectNode should not be null");
    assertEquals(
        "client_minicore_load", node.get("type").asText(), "Type should be client_minicore_load");
    assertEquals("JDBC", node.get("source").asText(), "Source should be JDBC");
    assertTrue(node.get("success").asBoolean(), "Success should be true");
    assertEquals(
        TEST_LIBRARY_FILE, node.get("libraryFileName").asText(), "Library file name should match");
    assertEquals(TEST_VERSION, node.get("coreVersion").asText(), "Core version should match");
    assertNull(node.get("error"), "Error should not be present on success");

    assertTrue(node.has("loadLogs"), "Should have loadLogs array");
    assertEquals(3, node.get("loadLogs").size(), "Should have 3 log entries");
    assertEquals("log1", node.get("loadLogs").get(0).asText());
    assertEquals("log2", node.get("loadLogs").get(1).asText());
    assertEquals("log3", node.get("loadLogs").get(2).asText());
  }

  @Test
  public void testInBandTelemetryNodeFromFailedLoad() {
    String errorMessage = "Failed to load library";
    List<String> logs = Arrays.asList("starting", "failed");
    MinicoreLoadResult result =
        MinicoreLoadResult.failure(
            errorMessage, TEST_LIBRARY_FILE, new RuntimeException("test"), logs);
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    ObjectNode node = telemetry.toInBandTelemetryNode();

    assertNotNull(node, "ObjectNode should not be null");
    assertEquals(
        "client_minicore_load", node.get("type").asText(), "Type should be client_minicore_load");
    assertEquals("JDBC", node.get("source").asText(), "Source should be JDBC");
    assertFalse(node.get("success").asBoolean(), "Success should be false");
    assertEquals(
        TEST_LIBRARY_FILE, node.get("libraryFileName").asText(), "Library file name should match");
    assertNull(node.get("coreVersion"), "Core version should not be present on failure");
    assertEquals(
        MinicoreLoadError.FAILED_TO_LOAD.getMessage(),
        node.get("error").asText(),
        "Error should be enum message");

    assertTrue(node.has("loadLogs"), "Should have loadLogs array");
  }

  @Test
  public void testInBandTelemetryNodeWhenStillLoading() {
    Minicore.resetForTesting();
    MinicoreTelemetry telemetry = MinicoreTelemetry.create();
    ObjectNode node = telemetry.toInBandTelemetryNode();

    assertNotNull(node, "ObjectNode should not be null");
    assertEquals("client_minicore_load", node.get("type").asText());
    assertEquals("JDBC", node.get("source").asText());
    assertFalse(node.get("success").asBoolean(), "Success should be false");
    assertEquals(MinicoreLoadError.STILL_LOADING.getMessage(), node.get("error").asText());
  }

  @Test
  public void testInBandTelemetryNodeWithEmptyLogs() {
    MinicoreLoadResult result =
        MinicoreLoadResult.success(TEST_LIBRARY_FILE, null, TEST_VERSION, new ArrayList<>());
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result);
    ObjectNode node = telemetry.toInBandTelemetryNode();

    assertNotNull(node, "ObjectNode should not be null");
    assertFalse(node.has("loadLogs"), "Should not have loadLogs when empty");
  }
}
