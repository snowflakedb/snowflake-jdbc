/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.StmtUtil.mapper;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.annotations.RunOnLinuxOrMac;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@Nested
@Tag(TestTags.CORE)
class FileCacheManagerTest extends BaseJDBCTest {

  private static final String CACHE_FILE_NAME = "temporary_credential.json";
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;

  private FileCacheManager fileCacheManager;
  private File cacheFile;

  @BeforeEach
  public void setup() throws IOException {
    fileCacheManager =
        FileCacheManager.builder()
            .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
            .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
            .setBaseCacheFileName(CACHE_FILE_NAME)
            .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
            .build();
    cacheFile = createCacheFile();
  }

  @AfterEach
  public void clean() throws IOException {
    if (Files.exists(cacheFile.toPath())) {
      Files.delete(cacheFile.toPath());
    }
    if (Files.exists(cacheFile.getParentFile().toPath())) {
      Files.delete(cacheFile.getParentFile().toPath());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "rwx------,rwx------,false",
    "rw-------,rwx------,true",
    "rw-------,rwx--xrwx,false",
    "r-x------,rwx------,false",
    "r--------,rwx------,true",
    "rwxrwx---,rwx------,false",
    "rwxrw----,rwx------,false",
    "rwxr-x---,rwx------,false",
    "rwxr-----,rwx------,false",
    "rwx-wx---,rwx------,false",
    "rwx-w----,rwx------,false",
    "rwx--x---,rwx------,false",
    "rwx---rwx,rwx------,false",
    "rwx---rw-,rwx------,false",
    "rwx---r-x,rwx------,false",
    "rwx---r--,rwx------,false",
    "rwx----wx,rwx------,false",
    "rwx----w-,rwx------,false",
    "rwx-----x,rwx------,false"
  })
  @RunOnLinuxOrMac
  public void throwWhenReadCacheFileWithPermissionDifferentThanReadWriteForUserTest(
      String permission, String parentDirectoryPermissions, boolean isSucceed) throws IOException {
    fileCacheManager.overrideCacheFile(cacheFile);
    Files.setPosixFilePermissions(cacheFile.toPath(), PosixFilePermissions.fromString(permission));
    Files.setPosixFilePermissions(cacheFile.getParentFile().toPath(), PosixFilePermissions.fromString(parentDirectoryPermissions));
    if (isSucceed) {
      assertDoesNotThrow(() -> fileCacheManager.readCacheFile());
    } else {
      SecurityException ex =
          assertThrows(SecurityException.class, () -> fileCacheManager.readCacheFile());
      assertTrue(ex.getMessage().contains("is wider than allowed only to the owner"));
    }
  }

  @Test
  @RunOnLinuxOrMac
  public void throwWhenOverrideCacheFileHasDifferentOwnerThanCurrentUserTest() {
    try (MockedStatic<FileUtil> fileUtilMock =
        Mockito.mockStatic(FileUtil.class, Mockito.CALLS_REAL_METHODS)) {
      fileUtilMock.when(() -> FileUtil.getFileOwnerName(isA(Path.class))).thenReturn("anotherUser");
      SecurityException ex =
          assertThrows(SecurityException.class, () -> fileCacheManager.readCacheFile());
      assertTrue(ex.getMessage().contains("The file owner is different than current user"));
    }
  }

  @Test
  @RunOnLinuxOrMac
  public void notThrowForToWidePermissionsWhenOnlyOwnerPermissionsSetFalseTest()
      throws IOException {
    fileCacheManager.setOnlyOwnerPermissions(false);
    Files.setPosixFilePermissions(cacheFile.toPath(), PosixFilePermissions.fromString("rwxrwx---"));
    assertDoesNotThrow(() -> fileCacheManager.readCacheFile());
  }

  @Test
  @RunOnLinuxOrMac
  public void throwWhenOverrideCacheFileNotFound() {
    Path wrongPath =
        Paths.get(systemGetProperty("user.home"), ".cache", "snowflake2", "wrongFileName");
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> fileCacheManager.overrideCacheFile(wrongPath.toFile()));
    assertTrue(
        ex.getMessage()
            .contains(
                "Unable to access the file/directory to check the permissions. Error: java.nio.file.NoSuchFileException:"));
  }

  @Test
  public void shouldCreateCacheDirForLinuxXDG() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic  = Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetEnv("XDG_CACHE_HOME")).thenReturn("/XDG/Cache/");
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
  public void shouldCreateCacheDirForLinuxWithoutXDG() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic  = Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetEnv("XDG_CACHE_HOME")).thenReturn(null);
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetProperty("user.home")).thenReturn("/User/Home");
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
  public void shouldCreateCacheDirForWindows() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.WINDOWS);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic  = Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetProperty("user.home")).thenReturn("/User/Home");
        try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
          fileUtilMockedStatic.when(() -> FileUtil.isWritable("/User/Home")).thenReturn(true);
          File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
          Assertions.assertNotNull(defaultCacheDir);
          Assertions.assertEquals("/User/Home/AppData/Local/Snowflake/Caches", defaultCacheDir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  public void shouldCreateCacheDirForMacOS() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.MAC);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic  = Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetProperty("user.home")).thenReturn("/User/Home");
        try (MockedStatic<FileUtil> fileUtilMockedStatic = Mockito.mockStatic(FileUtil.class)) {
          fileUtilMockedStatic.when(() -> FileUtil.isWritable("/User/Home")).thenReturn(true);
          File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
          Assertions.assertNotNull(defaultCacheDir);
          Assertions.assertEquals("/User/Home/Library/Caches/Snowflake", defaultCacheDir.getAbsolutePath());
        }
      }
    }
  }

  @Test
  public void shouldReturnNullWhenNoHomeDirSet() {
    try (MockedStatic<Constants> constantsMockedStatic = Mockito.mockStatic(Constants.class)) {
      constantsMockedStatic.when(Constants::getOS).thenReturn(Constants.OS.LINUX);
      try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic  = Mockito.mockStatic(SnowflakeUtil.class)) {
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetEnv("XDG_CACHE_HOME")).thenReturn(null);
        snowflakeUtilMockedStatic.when(() -> SnowflakeUtil.systemGetProperty("user.home")).thenReturn(null);
        File defaultCacheDir = FileCacheManager.getDefaultCacheDir();
        Assertions.assertNull(defaultCacheDir);
      }
    }
  }

  private File createCacheFile() {
    Path cacheFile =
        Paths.get(systemGetProperty("user.home"), ".cache", "snowflake2", CACHE_FILE_NAME);
    try {
      if (Files.exists(cacheFile)) {
        Files.delete(cacheFile);
      }
      Files.createDirectories(cacheFile.getParent(),
              PosixFilePermissions.asFileAttribute(
                      Stream.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE)
                              .collect(Collectors.toSet())));
      Files.createFile(
          cacheFile,
          PosixFilePermissions.asFileAttribute(
              Stream.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
                  .collect(Collectors.toSet())));
      ObjectNode cacheContent = mapper.createObjectNode();
      cacheContent.put("token", "tokenValue");
      fileCacheManager.overrideCacheFile(cacheFile.toFile());
      fileCacheManager.writeCacheFile(cacheContent);
      return cacheFile.toFile();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
