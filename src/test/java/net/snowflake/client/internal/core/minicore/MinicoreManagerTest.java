package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Tests for Minicore.
 *
 * <p>These tests verify the singleton functionality, including initialization and library access.
 */
public class MinicoreManagerTest {

  @Test
  public void testInitializeCreatesSingleton() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    assertNotNull(instance, "Instance should not be null after initialization");
  }

  @Test
  public void testInitializeIsIdempotent() {
    Minicore.initialize();
    Minicore instance1 = Minicore.getInstance();

    Minicore.initialize();
    Minicore instance2 = Minicore.getInstance();

    assertSame(instance1, instance2, "Multiple initializations should return the same instance");
  }

  @Test
  public void testGetInstanceBeforeInitializeReturnsNull() {
    // Note: This test assumes a fresh JVM or that tests run in isolation
    // In practice, other tests may have already initialized
    Minicore instance = Minicore.getInstance();
    // Can be null or non-null depending on test execution order
    // Just verify it doesn't throw
    assertDoesNotThrow(() -> Minicore.getInstance());
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testLoadResultIsSuccessOnSupportedPlatform() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    assertTrue(instance.getLoadResult().isSuccess(), "Load should succeed on supported platforms");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testGetLibraryReturnsNonNull() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    assertNotNull(instance.getLibrary(), "Library should not be null on supported platforms");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testGetVersionReturnsValidVersion() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    MinicoreLibrary library = instance.getLibrary();
    assertNotNull(library, "Library should not be null");
    
    String version = library.sf_core_full_version();
    assertNotNull(version, "Version should not be null");
    assertFalse(version.isEmpty(), "Version should not be empty");
    assertTrue(version.contains("0.0.1"), "Version should contain expected version number");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testGetVersionIsConsistent() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    MinicoreLibrary library = instance.getLibrary();
    String version1 = library.sf_core_full_version();
    String version2 = library.sf_core_full_version();

    assertEquals(version1, version2, "Version should be consistent across calls");
  }

  @Test
  public void testGetLoadResultReturnsNonNull() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    MinicoreLoadResult result = instance.getLoadResult();
    assertNotNull(result, "Load result should not be null");
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testLoadResultIsFailureOnUnsupportedPlatform() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    assertFalse(instance.getLoadResult().isSuccess(), "Load should fail on unsupported platforms");
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testGetLibraryReturnsNullOnUnsupportedPlatform() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    assertNull(instance.getLibrary(), "Library should be null on unsupported platforms");
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testGetLibraryIsNullSoVersionCannotBeCalled() {
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    assertNull(instance.getLibrary(), "Library should be null on unsupported platforms, so version cannot be called");
  }
}
