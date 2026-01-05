package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.Constants.Architecture;
import net.snowflake.client.core.Constants.OS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.mockito.MockedStatic;

public class MinicorePlatformTest {

  private static final List<TestPlatformConfig> SUPPORTED_PLATFORMS =
      Arrays.asList(
          // Linux glibc
          new TestPlatformConfig(
              OS.LINUX, Architecture.X86_64, LibcDetector.LibcVariant.GLIBC, false),
          new TestPlatformConfig(
              OS.LINUX, Architecture.AARCH64, LibcDetector.LibcVariant.GLIBC, false),
          // Linux musl
          new TestPlatformConfig(
              OS.LINUX, Architecture.X86_64, LibcDetector.LibcVariant.MUSL, false),
          new TestPlatformConfig(
              OS.LINUX, Architecture.AARCH64, LibcDetector.LibcVariant.MUSL, false),
          // macOS
          new TestPlatformConfig(
              OS.MAC, Architecture.X86_64, LibcDetector.LibcVariant.UNSUPPORTED, false),
          new TestPlatformConfig(
              OS.MAC, Architecture.AARCH64, LibcDetector.LibcVariant.UNSUPPORTED, false),
          // Windows
          new TestPlatformConfig(
              OS.WINDOWS, Architecture.X86_64, LibcDetector.LibcVariant.UNSUPPORTED, false),
          new TestPlatformConfig(
              OS.WINDOWS, Architecture.AARCH64, LibcDetector.LibcVariant.UNSUPPORTED, false),
          // AIX (detected as LINUX but isAix=true)
          new TestPlatformConfig(
              OS.LINUX, Architecture.PPC64, LibcDetector.LibcVariant.UNSUPPORTED, true));

  @Test
  public void testPlatformDetection() {
    MinicorePlatform platform = new MinicorePlatform();

    assertNotNull(platform.getOsName(), "OS name should not be null");
    assertNotNull(platform.getOsArch(), "OS architecture should not be null");
  }

  @Test
  @EnabledOnOs(LINUX)
  public void testLinuxPlatformFileName() {
    MinicorePlatform platform = new MinicorePlatform();

    String fileName = platform.getLibraryFileName();
    assertNotNull(fileName, "Library file name should not be null");
    assertTrue(
        fileName.matches("libsf_mini_core_linux_(x86_64|aarch64)_(glibc|musl)\\.so"),
        "Linux library should be 'libsf_mini_core_linux_{arch}_{libc}.so', got: " + fileName);
  }

  @Test
  @EnabledOnOs(MAC)
  public void testMacOSPlatformFileName() {
    MinicorePlatform platform = new MinicorePlatform();

    String fileName = platform.getLibraryFileName();
    assertNotNull(fileName, "Library file name should not be null");
    assertTrue(
        fileName.equals("libsf_mini_core_macos_x86_64.dylib")
            || fileName.equals("libsf_mini_core_macos_aarch64.dylib"),
        "macOS library should be 'libsf_mini_core_macos_{x86_64|aarch64}.dylib', got: " + fileName);
  }

  @Test
  @EnabledOnOs(WINDOWS)
  public void testWindowsPlatformFileName() {
    MinicorePlatform platform = new MinicorePlatform();

    String fileName = platform.getLibraryFileName();
    assertNotNull(fileName, "Library file name should not be null");
    assertTrue(
        fileName.equals("libsf_mini_core_windows_x86_64.dll")
            || fileName.equals("libsf_mini_core_windows_aarch64.dll"),
        "Windows library should be 'libsf_mini_core_windows_{x86_64|aarch64}.dll', got: "
            + fileName);
  }

  @Test
  public void testIsSupportedOnCurrentPlatform() {
    MinicorePlatform platform = new MinicorePlatform();
    String platformId = platform.getPlatformIdentifier();

    if (platformId != null
        && (platformId.startsWith("linux-")
            || platformId.startsWith("macos-")
            || platformId.startsWith("windows-"))) {
      assertTrue(platform.isSupported(), "Standard platforms should be supported: " + platformId);
    }
  }

  @Test
  public void testAllSupportedPlatformsHaveMatchingResourceFiles()
      throws IOException, URISyntaxException {
    // Generate library paths from all supported platform configs using mocking
    List<String> platformPaths = new ArrayList<>();
    for (TestPlatformConfig config : SUPPORTED_PLATFORMS) {
      String path = getLibraryPathForConfig(config);
      platformPaths.add(path);
    }

    // Get actual files from minicore directory
    URL resourceDir = MinicorePlatform.class.getResource("/minicore");
    assertNotNull(resourceDir, "minicore resource directory should exist");

    Path minicorePath = Paths.get(resourceDir.toURI());
    Set<String> actualFiles;
    try (Stream<Path> paths = Files.list(minicorePath)) {
      actualFiles =
          paths
              .map(p -> p.getFileName().toString())
              .filter(f -> f.endsWith(".so") || f.endsWith(".dylib") || f.endsWith(".dll"))
              .map(f -> "/minicore/" + f)
              .collect(Collectors.toSet());
    }

    // Compare sizes
    assertEquals(
        SUPPORTED_PLATFORMS.size(),
        actualFiles.size(),
        String.format(
            "Number of supported platforms (%d) should match number of resource files (%d).\n"
                + "Platform paths: %s\n"
                + "Actual files: %s",
            SUPPORTED_PLATFORMS.size(), actualFiles.size(), platformPaths, actualFiles));

    // Check each platform path has a matching file
    for (String platformPath : platformPaths) {
      assertTrue(
          actualFiles.contains(platformPath),
          String.format(
              "Platform path '%s' should exist in resource files.\nActual files: %s",
              platformPath, actualFiles));
    }
  }

  private String getLibraryPathForConfig(TestPlatformConfig config) {
    try (MockedStatic<Constants> constantsMock = mockStatic(Constants.class);
        MockedStatic<LibcDetector> libcMock = mockStatic(LibcDetector.class)) {

      constantsMock.when(Constants::getOS).thenReturn(config.os);
      constantsMock.when(Constants::getArchitecture).thenReturn(config.arch);
      constantsMock.when(Constants::isAix).thenReturn(config.isAix);
      libcMock.when(LibcDetector::detectLibcVariant).thenReturn(config.libcVariant);

      MinicorePlatform platform = new MinicorePlatform();
      assertTrue(platform.isSupported(), "Platform should be supported: " + config);
      return platform.getLibraryPath();
    }
  }

  private static class TestPlatformConfig {
    final OS os;
    final Architecture arch;
    final LibcDetector.LibcVariant libcVariant;
    final boolean isAix;

    TestPlatformConfig(
        OS os, Architecture arch, LibcDetector.LibcVariant libcVariant, boolean isAix) {
      this.os = os;
      this.arch = arch;
      this.libcVariant = libcVariant;
      this.isAix = isAix;
    }
  }
}
