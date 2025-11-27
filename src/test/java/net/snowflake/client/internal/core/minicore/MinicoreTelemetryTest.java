package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Tests for MinicoreTelemetry.
 *
 * <p>These tests verify telemetry data generation from load results, including ISA detection,
 * version information, error reporting, and log aggregation.
 */
public class MinicoreTelemetryTest {

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testTelemetryFromSuccessfulLoad() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      String version = loader.getLibrary().sf_core_full_version();
      MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);

      assertNotNull(telemetry, "Telemetry should not be null");
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testTelemetryToMapContainsISA() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      String version = loader.getLibrary().sf_core_full_version();
      MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);
      Map<String, Object> map = telemetry.toMap();

      assertTrue(map.containsKey("ISA"), "Telemetry should contain ISA");
      assertNotNull(map.get("ISA"), "ISA should not be null");
      assertTrue(map.get("ISA").toString().matches("amd64|arm64|ppc64|x86"), 
          "ISA should be a valid architecture: " + map.get("ISA"));
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testTelemetryToMapContainsCoreVersion() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      String version = loader.getLibrary().sf_core_full_version();
      MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);
      Map<String, Object> map = telemetry.toMap();

      assertTrue(map.containsKey("CORE_VERSION"), "Telemetry should contain CORE_VERSION");
      assertNotNull(map.get("CORE_VERSION"), "CORE_VERSION should not be null");
      assertTrue(map.get("CORE_VERSION").toString().contains("0.0.1"), 
          "CORE_VERSION should contain version number");
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testTelemetryToMapContainsLogs() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      String version = loader.getLibrary().sf_core_full_version();
      MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);
      Map<String, Object> map = telemetry.toMap();

      assertTrue(map.containsKey("CORE_LOAD_LOGS"), "Telemetry should contain CORE_LOAD_LOGS");
      assertNotNull(map.get("CORE_LOAD_LOGS"), "CORE_LOAD_LOGS should not be null");
      assertTrue(map.get("CORE_LOAD_LOGS") instanceof java.util.List, 
          "CORE_LOAD_LOGS should be a List");
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testTelemetryDoesNotContainErrorOnSuccess() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      String version = loader.getLibrary().sf_core_full_version();
      MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);
      Map<String, Object> map = telemetry.toMap();

      assertFalse(map.containsKey("CORE_LOAD_ERROR"), 
          "Telemetry should not contain CORE_LOAD_ERROR on success");
    }
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testTelemetryFromFailedLoad() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, null);
    Map<String, Object> map = telemetry.toMap();

    assertTrue(map.containsKey("CORE_LOAD_ERROR"), 
        "Telemetry should contain CORE_LOAD_ERROR on failure");
    assertNotNull(map.get("CORE_LOAD_ERROR"), "CORE_LOAD_ERROR should not be null");
    assertFalse(map.containsKey("CORE_VERSION"), 
        "Telemetry should not contain CORE_VERSION on failure");
  }

  @Test
  public void testTelemetryDoesNotContainOSOrOSVersion() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();
    
    String version = null;
    if (result.isSuccess() && loader.getLibrary() != null) {
      version = loader.getLibrary().sf_core_full_version();
    }
    
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);
    Map<String, Object> map = telemetry.toMap();

    assertFalse(map.containsKey("OS"), 
        "Telemetry should not contain OS (already in SessionUtil)");
    assertFalse(map.containsKey("OS_VERSION"), 
        "Telemetry should not contain OS_VERSION (already in SessionUtil)");
  }

  @Test
  public void testTelemetryFromNullLoadResult() {
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(null, null);
    Map<String, Object> map = telemetry.toMap();

    assertNotNull(telemetry, "Telemetry should not be null even with null load result");
    assertTrue(map.containsKey("CORE_LOAD_ERROR"), 
        "Telemetry should contain error when load result is null");
    assertEquals("MinicoreManager not initialized", map.get("CORE_LOAD_ERROR"),
        "Error message should indicate not initialized");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testTelemetryLogsAreTimestamped() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    if (result.isSuccess()) {
      String version = loader.getLibrary().sf_core_full_version();
      MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);
      Map<String, Object> map = telemetry.toMap();

      @SuppressWarnings("unchecked")
      java.util.List<String> logs = (java.util.List<String>) map.get("CORE_LOAD_LOGS");
      
      assertNotNull(logs, "Logs should not be null");
      assertFalse(logs.isEmpty(), "Logs should not be empty");
      
      for (String log : logs) {
        assertTrue(log.matches("^\\[\\d+\\.\\d{6}ms\\] .*"), 
            "Each log should be timestamped: " + log);
      }
    }
  }

  @Test
  public void testTelemetryToStringDoesNotThrow() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();
    
    String version = null;
    if (result.isSuccess() && loader.getLibrary() != null) {
      version = loader.getLibrary().sf_core_full_version();
    }
    
    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(result, version);

    assertDoesNotThrow(() -> telemetry.toString(), 
        "toString should not throw");
    assertNotNull(telemetry.toString(), "toString should not return null");
  }
}
