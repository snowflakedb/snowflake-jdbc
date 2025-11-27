package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Tests for MinicorePlatform.
 *
 * <p>These tests verify platform detection, library path construction, and resource availability
 * checking.
 */
public class MinicorePlatformTest {

  @Test
  public void testPlatformDetection() {
    MinicorePlatform platform = new MinicorePlatform();

    assertNotNull(platform.getOsName(), "OS name should not be null");
    assertNotNull(platform.getOsArch(), "OS architecture should not be null");
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  public void testLinuxPlatformIdentifier() {
    MinicorePlatform platform = new MinicorePlatform();

    String platformId = platform.getPlatformIdentifier();
    assertNotNull(platformId, "Platform identifier should not be null");
    assertTrue(platformId.startsWith("linux-"), "Linux platform should start with 'linux-'");
    assertTrue(platformId.equals("linux-x86_64") || platformId.equals("linux-aarch64"), 
        "Linux platform should be x86_64 or aarch64");
  }

  @Test
  @EnabledOnOs(OS.MAC)
  public void testMacOSPlatformIdentifier() {
    MinicorePlatform platform = new MinicorePlatform();

    String platformId = platform.getPlatformIdentifier();
    assertNotNull(platformId, "Platform identifier should not be null");
    assertTrue(platformId.startsWith("macos-"), "macOS platform should start with 'macos-'");
    assertTrue(platformId.equals("macos-x86_64") || platformId.equals("macos-aarch64"), 
        "macOS platform should be x86_64 or aarch64");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testIsSupportedOnSupportedPlatforms() {
    MinicorePlatform platform = new MinicorePlatform();

    assertTrue(platform.isSupported(), "Platform should be supported on Linux and macOS");
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testIsNotSupportedOnWindows() {
    MinicorePlatform platform = new MinicorePlatform();

    assertFalse(platform.isSupported(), "Platform should not be supported on Windows");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testGetLibraryPathOnSupportedPlatform() {
    MinicorePlatform platform = new MinicorePlatform();

    String libraryPath = platform.getLibraryPath();
    assertNotNull(libraryPath, "Library path should not be null on supported platforms");
    assertTrue(libraryPath.startsWith("/minicore/"), "Library path should start with /minicore/");
    assertTrue(libraryPath.contains("sf_mini_core"), "Library path should contain library name");
    assertTrue(libraryPath.contains("0.0.1_SNAPSHOT"), "Library path should contain version");
    assertTrue(libraryPath.contains("47a8f3d"), "Library path should contain git hash");
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testGetLibraryPathThrowsOnUnsupportedPlatform() {
    MinicorePlatform platform = new MinicorePlatform();

    assertThrows(UnsupportedOperationException.class, platform::getLibraryPath,
        "getLibraryPath should throw on unsupported platforms");
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  public void testGetLibraryFileNameOnLinux() {
    MinicorePlatform platform = new MinicorePlatform();

    String fileName = platform.getLibraryFileName();
    assertEquals("libsf_mini_core.so", fileName, "Linux library should have .so extension");
  }

  @Test
  @EnabledOnOs(OS.MAC)
  public void testGetLibraryFileNameOnMacOS() {
    MinicorePlatform platform = new MinicorePlatform();

    String fileName = platform.getLibraryFileName();
    assertEquals("libsf_mini_core.dylib", fileName, "macOS library should have .dylib extension");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  public void testResourcePathMatchesActualResource() {
    MinicorePlatform platform = new MinicorePlatform();

    if (platform.isSupported()) {
      String resourcePath = platform.getLibraryPath();
      assertNotNull(MinicorePlatform.class.getResource(resourcePath),
          "Resource should exist at the computed path: " + resourcePath);
    }
  }

  @Test
  public void testGetOsReturnsValidEnum() {
    MinicorePlatform platform = new MinicorePlatform();

    assertNotNull(platform.getOs(), "OS enum should not be null");
  }

  @Test
  public void testPlatformIdentifierFormat() {
    MinicorePlatform platform = new MinicorePlatform();

    String platformId = platform.getPlatformIdentifier();
    if (platformId != null) {
      assertTrue(platformId.matches("\\w+-\\w+"), 
          "Platform identifier should match format 'os-arch': " + platformId);
    }
  }
}
