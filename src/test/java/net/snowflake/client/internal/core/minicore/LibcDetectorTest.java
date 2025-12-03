package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class LibcDetectorTest {

  @Test
  @EnabledOnOs(OS.LINUX)
  public void testDetectLibcVariantOnLinux() {
    LibcDetector.LibcVariant variant = LibcDetector.detectLibcVariant();

    assertTrue(
        variant == LibcDetector.LibcVariant.GLIBC || variant == LibcDetector.LibcVariant.MUSL,
        "On Linux, variant should be GLIBC or MUSL, got: " + variant);
  }

  @Test
  @EnabledOnOs({OS.MAC, OS.WINDOWS})
  public void testDetectLibcVariantOnNonLinux() {
    LibcDetector.LibcVariant variant = LibcDetector.detectLibcVariant();

    assertEquals(
        LibcDetector.LibcVariant.UNSUPPORTED,
        variant,
        "On non-Linux platforms, variant should be UNSUPPORTED");
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  public void testPlatformIdentifierIncludesLibcVariantOnLinux() {
    MinicorePlatform platform = new MinicorePlatform();
    String platformId = platform.getPlatformIdentifier();

    assertNotNull(platformId, "Platform identifier should not be null");

    // On Linux, should be in format: linux-{arch}-{libc}
    assertTrue(
        platformId.contains("-glibc") || platformId.contains("-musl"),
        "Platform identifier on Linux should include libc variant: " + platformId);

    // Should be three parts: os-arch-libc
    String[] parts = platformId.split("-");
    assertEquals(
        3,
        parts.length,
        "Platform identifier on Linux should have 3 parts (os-arch-libc): " + platformId);
  }

  @Test
  @EnabledOnOs({OS.MAC, OS.WINDOWS})
  public void testPlatformIdentifierExcludesLibcVariantOnNonLinux() {
    MinicorePlatform platform = new MinicorePlatform();
    String platformId = platform.getPlatformIdentifier();

    assertNotNull(platformId, "Platform identifier should not be null");

    // On non-Linux, should NOT include libc variant
    String[] parts = platformId.split("-");
    assertEquals(
        2,
        parts.length,
        "Platform identifier on non-Linux should have 2 parts (os-arch): " + platformId);
  }

  @Test
  public void testLibcVariantIdentifiers() {
    assertEquals("glibc", LibcDetector.LibcVariant.GLIBC.getIdentifier());
    assertEquals("musl", LibcDetector.LibcVariant.MUSL.getIdentifier());
  }
}
