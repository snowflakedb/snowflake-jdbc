package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/** Tests for LibcDetector - focused on libc variant detection logic. */
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
  public void testLibcVariantIdentifiers() {
    assertEquals("glibc", LibcDetector.LibcVariant.GLIBC.getIdentifier());
    assertEquals("musl", LibcDetector.LibcVariant.MUSL.getIdentifier());
    assertEquals("unsupported", LibcDetector.LibcVariant.UNSUPPORTED.getIdentifier());
  }
}
