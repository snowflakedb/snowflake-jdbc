package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Core test suite for Minicore initialization and functionality.
 *
 * <p>This test suite verifies that minicore loads successfully on supported platforms and provides
 * detailed diagnostics on failure. When run on CI across different platforms (Linux, macOS, etc.),
 * these tests automatically verify minicore works correctly on all supported platforms.
 */
public class MinicoreTest {

  @Test
  public void testMinicoreInitializesSuccessfully() {
    // GIVEN - Initialize minicore
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();
    MinicorePlatform platform = new MinicorePlatform();

    // THEN - Instance should be created
    assertNotNull(instance, "Minicore instance should not be null after initialization");

    // AND - Load result should exist
    MinicoreLoadResult result = instance.getLoadResult();
    assertNotNull(result, "Load result should not be null");

    // AND - Build detailed error message in case of failure
    StringBuilder errorMessage = new StringBuilder();
    errorMessage
        .append("Minicore should load successfully on platform: ")
        .append(platform.getPlatformIdentifier());
    errorMessage.append("\nPlatform supported: ").append(platform.isSupported());
    errorMessage.append("\nOS: ").append(platform.getOsName());
    errorMessage.append("\nArch: ").append(platform.getOsArch());

    if (!result.isSuccess()) {
      errorMessage.append("\n\n=== LOAD FAILURE DETAILS ===");
      errorMessage.append("\nError: ").append(result.getErrorMessage());
      if (result.getException() != null) {
        errorMessage.append("\nException: ").append(result.getException().getClass().getName());
        errorMessage.append(": ").append(result.getException().getMessage());
      }
      errorMessage.append("\nLibrary file: ").append(result.getLibraryFileName());
      errorMessage.append("\n\nLoad Logs:");
      for (String log : result.getLogs()) {
        errorMessage.append("\n  ").append(log);
      }
    }

    // AND - Loading should succeed
    assertTrue(result.isSuccess(), errorMessage.toString());
    assertNull(result.getErrorMessage(), "Error message should be null on success");
    assertNull(result.getException(), "Exception should be null on success");

    // AND - Library should be accessible
    MinicoreLibrary library = instance.getLibrary();
    assertNotNull(library, "Library should not be null after successful load");

    // AND - Native function should work
    String version = library.sf_core_full_version();
    assertNotNull(version, "Version should not be null");
    assertFalse(version.isEmpty(), "Version should not be empty");
    assertTrue(
        version.contains("0.0.1"),
        "Version should contain expected version number, got: " + version);

    // AND - Version should be in load result
    assertEquals(version, result.getCoreVersion(), "Version in result should match library call");
  }

  @Test
  public void testMinicoreInitializationIsIdempotent() {
    // GIVEN - Initialize minicore twice
    Minicore.initialize();
    Minicore instance1 = Minicore.getInstance();

    Minicore.initialize();
    Minicore instance2 = Minicore.getInstance();

    // THEN - Should return the same singleton instance
    assertSame(instance1, instance2, "Multiple initializations should return the same instance");

    // AND - Load result should be the same
    assertSame(
        instance1.getLoadResult(),
        instance2.getLoadResult(),
        "Load result should be the same across calls");
  }

  @Test
  public void testMinicoreLibraryFunctionConsistency() {
    // GIVEN - Initialized minicore
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    MinicoreLoadResult result = instance.getLoadResult();
    assertTrue(result.isSuccess(), "This test requires successful minicore load");

    MinicoreLibrary library = instance.getLibrary();
    assertNotNull(library, "Library should not be null");

    // WHEN - Calling the same function multiple times
    String version1 = library.sf_core_full_version();
    String version2 = library.sf_core_full_version();
    String version3 = library.sf_core_full_version();

    // THEN - Results should be consistent
    assertEquals(version1, version2, "Version should be consistent across calls");
    assertEquals(version2, version3, "Version should be consistent across calls");
  }
}
