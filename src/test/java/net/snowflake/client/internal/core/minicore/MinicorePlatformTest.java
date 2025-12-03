package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  public void testLinuxPlatformIdentifierAndFileName() {
    MinicorePlatform platform = new MinicorePlatform();

    // Platform identifier - uses x86_64/aarch64
    String platformId = platform.getPlatformIdentifier();
    assertNotNull(platformId, "Platform identifier should not be null");
    assertTrue(platformId.startsWith("linux-"), "Linux platform should start with 'linux-'");

    // Linux should include libc variant (glibc or musl)
    assertTrue(
        platformId.matches("linux-(x86_64|aarch64)-(glibc|musl)"),
        "Linux platform should be 'linux-{arch}-{libc}', got: " + platformId);

    // Filename - uses same identifiers as platform
    String fileName = platform.getLibraryFileName();
    assertNotNull(fileName, "Library file name should not be null");
    assertTrue(
        fileName.matches("libsf_mini_core_linux_(x86_64|aarch64)_(glibc|musl)\\.so"),
        "Linux library should be 'libsf_mini_core_linux_{arch}_{libc}.so', got: " + fileName);
  }

  @Test
  @EnabledOnOs(OS.MAC)
  public void testMacOSPlatformIdentifierAndFileName() {
    MinicorePlatform platform = new MinicorePlatform();

    // Platform identifier - uses macos and x86_64/aarch64
    String platformId = platform.getPlatformIdentifier();
    assertNotNull(platformId, "Platform identifier should not be null");
    assertTrue(platformId.startsWith("macos-"), "macOS platform should start with 'macos-'");
    assertTrue(
        platformId.equals("macos-x86_64") || platformId.equals("macos-aarch64"),
        "macOS platform should be x86_64 or aarch64, got: " + platformId);

    // Filename - uses same identifiers as platform
    String fileName = platform.getLibraryFileName();
    assertNotNull(fileName, "Library file name should not be null");
    assertTrue(
        fileName.equals("libsf_mini_core_macos_x86_64.dylib")
            || fileName.equals("libsf_mini_core_macos_aarch64.dylib"),
        "macOS library should be 'libsf_mini_core_macos_{x86_64|aarch64}.dylib', got: " + fileName);
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  public void testWindowsPlatformIdentifierAndFileName() {
    MinicorePlatform platform = new MinicorePlatform();

    // Platform identifier - uses x86_64
    String platformId = platform.getPlatformIdentifier();
    assertNotNull(platformId, "Platform identifier should not be null");
    assertTrue(platformId.startsWith("windows-"), "Windows platform should start with 'windows-'");
    assertEquals("windows-x86_64", platformId, "Windows platform should be x86_64 (currently)");

    // Filename - uses lib prefix for unified naming
    String fileName = platform.getLibraryFileName();
    assertEquals(
        "libsf_mini_core_windows_x86_64.dll",
        fileName,
        "Windows library should be 'libsf_mini_core_windows_x86_64.dll'");
  }

  @Test
  public void testIsSupportedOnCurrentPlatform() {
    MinicorePlatform platform = new MinicorePlatform();
    String platformId = platform.getPlatformIdentifier();

    // Most common platforms should be supported
    if (platformId != null
        && (platformId.startsWith("linux-")
            || platformId.startsWith("macos-")
            || platformId.startsWith("windows-"))) {
      assertTrue(platform.isSupported(), "Standard platforms should be supported: " + platformId);
    }
  }

  @Test
  public void testGetLibraryPathOnSupportedPlatform() {
    MinicorePlatform platform = new MinicorePlatform();

    if (platform.isSupported()) {
      String libraryPath = platform.getLibraryPath();
      String fileName = platform.getLibraryFileName();

      assertNotNull(libraryPath, "Library path should not be null on supported platforms");

      // Flat structure: /minicore/{filename}
      assertEquals(
          "/minicore/" + fileName,
          libraryPath,
          "Library path should be flat structure: /minicore/{filename}");

      assertTrue(libraryPath.startsWith("/minicore/"), "Library path should start with /minicore/");
      assertTrue(
          libraryPath.contains("sf_mini_core"), "Library path should contain library base name");
    }
  }

  @Test
  public void testGetLibraryPathThrowsOnUnsupportedPlatform() {
    MinicorePlatform platform = new MinicorePlatform();

    if (!platform.isSupported()) {
      assertThrows(
          UnsupportedOperationException.class,
          platform::getLibraryPath,
          "getLibraryPath should throw on unsupported platforms");
    }
  }

  @Test
  public void testResourcePathMatchesActualResource() {
    MinicorePlatform platform = new MinicorePlatform();

    if (platform.isSupported()) {
      String resourcePath = platform.getLibraryPath();
      assertNotNull(
          MinicorePlatform.class.getResource(resourcePath),
          "Resource should exist at the computed path: " + resourcePath);
    }
  }
}
