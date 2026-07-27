package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isWindows;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

class FileCacheManagerBuilder {
  private static final SFLogger logger = SFLoggerFactory.getLogger(FileCacheManagerBuilder.class);

  private String cacheDirectorySystemProperty;
  private String cacheDirectoryEnvironmentVariable;
  private String baseCacheFileName;
  private long cacheFileLockExpirationInMilliseconds;
  private boolean onlyOwnerPermissions = true;

  FileCacheManagerBuilder() {}

  FileCacheManagerBuilder setCacheDirectorySystemProperty(String cacheDirectorySystemProperty) {
    this.cacheDirectorySystemProperty = cacheDirectorySystemProperty;
    return this;
  }

  FileCacheManagerBuilder setCacheDirectoryEnvironmentVariable(
      String cacheDirectoryEnvironmentVariable) {
    this.cacheDirectoryEnvironmentVariable = cacheDirectoryEnvironmentVariable;
    return this;
  }

  FileCacheManagerBuilder setBaseCacheFileName(String baseCacheFileName) {
    this.baseCacheFileName = baseCacheFileName;
    return this;
  }

  FileCacheManagerBuilder setCacheFileLockExpirationInSeconds(
      long cacheFileLockExpirationInSeconds) {
    this.cacheFileLockExpirationInMilliseconds = cacheFileLockExpirationInSeconds * 1000;
    return this;
  }

  FileCacheManagerBuilder setOnlyOwnerPermissions(boolean onlyOwnerPermissions) {
    this.onlyOwnerPermissions = onlyOwnerPermissions;
    return this;
  }

  FileCacheManager build() {
    String cacheDirPath =
        this.cacheDirectorySystemProperty != null
            ? systemGetProperty(this.cacheDirectorySystemProperty)
            : null;
    if (cacheDirPath == null) {
      try {
        cacheDirPath =
            this.cacheDirectoryEnvironmentVariable != null
                ? systemGetEnv(this.cacheDirectoryEnvironmentVariable)
                : null;
      } catch (Exception ex) {
        logger.debug(
            "Cannot get environment variable for cache directory, skip using cache", false);
        return new NoOpFileCacheManager();
      }
    }

    File cacheDir;
    if (cacheDirPath != null) {
      cacheDir = new File(cacheDirPath);
    } else {
      cacheDir = FileCacheUtil.getDefaultCacheDir();
    }
    if (cacheDir == null) {
      return new NoOpFileCacheManager();
    }
    if (!cacheDir.exists()) {
      try {
        if (!isWindows() && onlyOwnerPermissions) {
          Files.createDirectories(
              cacheDir.toPath(),
              PosixFilePermissions.asFileAttribute(
                  Stream.of(
                          PosixFilePermission.OWNER_READ,
                          PosixFilePermission.OWNER_WRITE,
                          PosixFilePermission.OWNER_EXECUTE)
                      .collect(Collectors.toSet())));
        } else {
          Files.createDirectories(cacheDir.toPath());
        }
      } catch (IOException e) {
        logger.info(
            "Failed to create the cache directory: {}. Ignored. {}",
            e.getMessage(),
            cacheDir.getAbsoluteFile());
        return new NoOpFileCacheManager();
      }
    }
    if (!cacheDir.exists()) {
      logger.debug("Cannot create the cache directory {}. Giving up.", cacheDir.getAbsolutePath());
      return new NoOpFileCacheManager();
    }
    logger.debug("Verified Directory {}", cacheDir.getAbsolutePath());

    File cacheFileTmp = new File(cacheDir, this.baseCacheFileName).getAbsoluteFile();
    try {
      if (!cacheFileTmp.exists()) {
        if (!isWindows() && onlyOwnerPermissions) {
          Files.createFile(
              cacheFileTmp.toPath(),
              PosixFilePermissions.asFileAttribute(
                  Stream.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
                      .collect(Collectors.toSet())));
        } else {
          Files.createFile(cacheFileTmp.toPath());
        }
        logger.debug("Successfully created a cache file {}", cacheFileTmp);
      } else {
        logger.debug("Cache file already exists {}", cacheFileTmp);
      }
      FileUtil.logFileUsage(cacheFileTmp, "Cache file creation", false);
      File cacheFile = cacheFileTmp.getCanonicalFile();
      File cacheLockFile = new File(cacheFile.getParentFile(), this.baseCacheFileName + ".lck");
      return new DefaultFileCacheManager(
          cacheFile,
          cacheLockFile,
          this.baseCacheFileName,
          this.onlyOwnerPermissions,
          this.cacheFileLockExpirationInMilliseconds);
    } catch (IOException | SecurityException ex) {
      logger.info(
          "Failed to touch the cache file: {}. Ignored. {}",
          ex.getMessage(),
          cacheFileTmp.getAbsoluteFile());
    }
    return new NoOpFileCacheManager();
  }
}
