package net.snowflake.client.core;

import java.io.File;
import net.snowflake.client.annotations.RunOnLinuxOrMac;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class FileCacheManagerDefaultDirTest {

  @Test
  @RunOnLinuxOrMac
  public void shouldCreateCacheDirForLinuxXDG() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic =
          Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetEnv("XDG_CACHE_HOME"))
            .thenReturn("/XDG/Cache/");
        try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
          fileUtilMockedStatic.when(() -> FileUtil.isWritable("/XDG/Cache/")).thenReturn(true);
          File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
          Assertions.assertNotNull(defaultCacheDir);
          Assertions.assertEquals("/XDG/Cache/snowflake", defaultCacheDir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  @RunOnLinuxOrMac
  public void shouldCreateCacheDirForLinuxWithoutXDG() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic =
          Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetEnv("XDG_CACHE_HOME"))
            .thenReturn(null);
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetProperty("user.home"))
            .thenReturn("/User/Home");
        try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
          fileUtilMockedStatic.when(() -> FileUtil.isWritable("/User/Home")).thenReturn(true);
          File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
          Assertions.assertNotNull(defaultCacheDir);
          Assertions.assertEquals("/User/Home/.cache/snowflake", defaultCacheDir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  @RunOnLinuxOrMac
  public void shouldCreateCacheDirForWindows() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.WINDOWS);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic =
          Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetProperty("user.home"))
            .thenReturn("/User/Home");
        try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
          fileUtilMockedStatic.when(() -> FileUtil.isWritable("/User/Home")).thenReturn(true);
          File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
          Assertions.assertNotNull(defaultCacheDir);
          Assertions.assertEquals(
              "/User/Home/AppData/Local/Snowflake/Caches", defaultCacheDir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  @RunOnLinuxOrMac
  public void shouldCreateCacheDirForMacOS() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.MAC);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic =
          Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetProperty("user.home"))
            .thenReturn("/User/Home");
        try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
          fileUtilMockedStatic.when(() -> FileUtil.isWritable("/User/Home")).thenReturn(true);
          File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
          Assertions.assertNotNull(defaultCacheDir);
          Assertions.assertEquals(
              "/User/Home/Library/Caches/Snowflake", defaultCacheDir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  @RunOnLinuxOrMac
  public void shouldReturnNullWhenNoHomeDirSet() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic =
          Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetEnv("XDG_CACHE_HOME"))
            .thenReturn(null);
        snowflakeUtilMockedStatic
            .when(() -> SnowflakeUtil.systemGetProperty("user.home"))
            .thenReturn(null);
        File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
        Assertions.assertNull(defaultCacheDir);
      }
    }
  }
}
