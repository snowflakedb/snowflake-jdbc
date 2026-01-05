package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Path;
import java.nio.file.Paths;
import net.snowflake.client.core.Constants;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class MinicoreLoaderTest {

  @Test
  public void testLoadLibraryIsCached() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result1 = loader.loadLibrary();
    MinicoreLoadResult result2 = loader.loadLibrary();

    assertSame(result1, result2, "Subsequent calls should return the same cached result");
  }

  @Test
  public void testLoadResultContainsAllFields() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();
    MinicorePlatform platform = new MinicorePlatform();

    assertNotNull(result.getLibraryFileName(), "Library file name should not be null");
    String expectedFileName = platform.getLibraryFileName();
    assertTrue(
        result.getLibraryFileName().contains("sf_mini_core"),
        "Library file name should contain base name");
    assertEquals(
        result.getLibraryFileName(),
        expectedFileName,
        "Library file name should match platform's expected file name");

    assertNotNull(result.getLogs(), "Logs should not be null");
    assertFalse(result.getLogs().isEmpty(), "Logs should contain entries");

    if (result.isSuccess()) {
      assertNull(result.getErrorMessage(), "Error message should be null on success");
      assertNull(result.getException(), "Exception should be null on success");
    } else {
      assertNotNull(result.getErrorMessage(), "Error message should be set on failure");
    }
  }

  @Test
  public void testHomeCacheDirectoryWindows() {
    String home = System.getProperty("user.home");
    try (MockedStatic<Constants> constantsMock = mockStatic(Constants.class)) {
      constantsMock.when(Constants::getOS).thenReturn(Constants.OS.WINDOWS);
      constantsMock.when(Constants::getArchitecture).thenReturn(Constants.Architecture.X86_64);

      MinicoreLoader loader = new MinicoreLoader();
      Path cacheDir = loader.getHomeCacheDirectory();

      Path expected = Paths.get(home, "AppData", "Local", "Snowflake", "Caches", "minicore");
      assertEquals(expected, cacheDir);
    }
  }

  @Test
  public void testHomeCacheDirectoryMac() {
    String home = System.getProperty("user.home");
    try (MockedStatic<Constants> constantsMock = mockStatic(Constants.class)) {
      constantsMock.when(Constants::getOS).thenReturn(Constants.OS.MAC);
      constantsMock.when(Constants::getArchitecture).thenReturn(Constants.Architecture.AARCH64);

      MinicoreLoader loader = new MinicoreLoader();
      Path cacheDir = loader.getHomeCacheDirectory();

      Path expected = Paths.get(home, "Library", "Caches", "Snowflake", "minicore");
      assertEquals(expected, cacheDir);
    }
  }

  @Test
  public void testHomeCacheDirectoryLinux() {
    String home = System.getProperty("user.home");
    try (MockedStatic<Constants> constantsMock = mockStatic(Constants.class)) {
      constantsMock.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      constantsMock.when(Constants::getArchitecture).thenReturn(Constants.Architecture.X86_64);
      constantsMock.when(Constants::isAix).thenReturn(false);

      MinicoreLoader loader = new MinicoreLoader();
      Path cacheDir = loader.getHomeCacheDirectory();

      Path expected = Paths.get(home, ".cache", "Snowflake", "minicore");
      assertEquals(expected, cacheDir);
    }
  }

  @Test
  public void testHomeCacheDirectoryReturnsNullWhenHomeNotSet() {
    String originalHome = System.getProperty("user.home");
    try {
      System.clearProperty("user.home");
      MinicoreLoader loader = new MinicoreLoader();
      Path cacheDir = loader.getHomeCacheDirectory();
      assertNull(cacheDir, "Cache directory should be null when home is not set");
    } finally {
      if (originalHome != null) {
        System.setProperty("user.home", originalHome);
      }
    }
  }
}
