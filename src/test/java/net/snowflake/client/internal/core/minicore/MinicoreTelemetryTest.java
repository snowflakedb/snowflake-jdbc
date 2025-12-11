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

  @Test
  public void testTelemetryFromSuccessfulLoad() {
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
  public void testTelemetryFromFailedLoad() {
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
    // CORE_LOAD_ERROR is only sent via in-band telemetry, not in CLIENT_ENVIRONMENT
    assertFalse(
        map.containsKey("CORE_LOAD_ERROR"),
        "CORE_LOAD_ERROR should not be in client environment map");
  }

  @Test
  public void testTelemetryFromNullMinicore() {
    MinicoreTelemetry telemetry = MinicoreTelemetry.from(null);
    Map<String, Object> map = telemetry.toClientEnvironmentTelemetryMap();

    assertNotNull(telemetry, "Telemetry should not be null");
    assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
    assertFalse(
        map.containsKey("CORE_LOAD_ERROR"),
        "CORE_LOAD_ERROR should not be in client environment map");

    // Check in-band telemetry has the correct error
    ObjectNode node = telemetry.toInBandTelemetryNode();
    assertEquals("Minicore not initialized", node.get("error").asText());
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

    // Check logs array
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
    assertEquals(errorMessage, node.get("error").asText(), "Error message should match");

    // Check logs array
    assertTrue(node.has("loadLogs"), "Should have loadLogs array");
    assertEquals(2, node.get("loadLogs").size(), "Should have 2 log entries");
  }

  @Test
  public void testInBandTelemetryNodeFromNullMinicore() {
    MinicoreTelemetry telemetry = MinicoreTelemetry.from(null);
    ObjectNode node = telemetry.toInBandTelemetryNode();

    assertNotNull(node, "ObjectNode should not be null");
    assertEquals("client_minicore_load", node.get("type").asText());
    assertEquals("JDBC", node.get("source").asText());
    assertFalse(node.get("success").asBoolean(), "Success should be false");
    assertEquals("Minicore not initialized", node.get("error").asText());
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
