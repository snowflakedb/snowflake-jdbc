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
import org.junit.jupiter.api.AfterEach;
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
  private static final long CACHE_EXPIRATION_IN_SECONDS = 86400L;
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
  }

  @ParameterizedTest
  @CsvSource({
    "rwx------,false",
    "rw-------,true",
    "r-x------,false",
    "r--------,true",
    "rwxrwx---,false",
    "rwxrw----,false",
    "rwxr-x---,false",
    "rwxr-----,false",
    "rwx-wx---,false",
    "rwx-w----,false",
    "rwx--x---,false",
    "rwx---rwx,false",
    "rwx---rw-,false",
    "rwx---r-x,false",
    "rwx---r--,false",
    "rwx----wx,false",
    "rwx----w-,false",
    "rwx-----x,false"
  })
  @RunOnLinuxOrMac
  public void throwWhenReadCacheFileWithPermissionDifferentThanReadWriteForUserTest(
      String permission, boolean isSucceed) throws IOException {
    fileCacheManager.overrideCacheFile(cacheFile);
    Files.setPosixFilePermissions(cacheFile.toPath(), PosixFilePermissions.fromString(permission));
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
                "Unable to access the file to check the permissions. Error: java.nio.file.NoSuchFileException:"));
  }

  private File createCacheFile() {
    Path cacheFile =
        Paths.get(systemGetProperty("user.home"), ".cache", "snowflake2", CACHE_FILE_NAME);
    try {
      if (Files.exists(cacheFile)) {
        Files.delete(cacheFile);
      }
      Files.createDirectories(cacheFile.getParent());
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
