/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class FileCacheManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(FileCacheManager.class);

  /** Object mapper for JSON encoding and decoding */
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  private static final Charset DEFAULT_FILE_ENCODING = StandardCharsets.UTF_8;

  private String cacheDirectorySystemProperty;
  private String cacheDirectoryEnvironmentVariable;
  private String baseCacheFileName;
  private long cacheExpirationInMilliseconds;
  private long cacheFileLockExpirationInMilliseconds;

  private File cacheFile;
  private File cacheLockFile;

  private File cacheDir;

  private FileCacheManager() {}

  static FileCacheManager builder() {
    return new FileCacheManager();
  }

  FileCacheManager setCacheDirectorySystemProperty(String cacheDirectorySystemProperty) {
    this.cacheDirectorySystemProperty = cacheDirectorySystemProperty;
    return this;
  }

  FileCacheManager setCacheDirectoryEnvironmentVariable(String cacheDirectoryEnvironmentVariable) {
    this.cacheDirectoryEnvironmentVariable = cacheDirectoryEnvironmentVariable;
    return this;
  }

  FileCacheManager setBaseCacheFileName(String baseCacheFileName) {
    this.baseCacheFileName = baseCacheFileName;
    return this;
  }

  FileCacheManager setCacheExpirationInSeconds(long cacheExpirationInSeconds) {
    // converting from seconds to milliseconds
    this.cacheExpirationInMilliseconds = cacheExpirationInSeconds * 1000;
    return this;
  }

  FileCacheManager setCacheFileLockExpirationInSeconds(long cacheFileLockExpirationInSeconds) {
    this.cacheFileLockExpirationInMilliseconds = cacheFileLockExpirationInSeconds * 1000;
    return this;
  }

  /**
   * Override the cache file.
   *
   * @param newCacheFile a file object to override the default one.
   */
  void overrideCacheFile(File newCacheFile) {
    this.cacheFile = newCacheFile;
    this.cacheDir = newCacheFile.getParentFile();
    this.baseCacheFileName = newCacheFile.getName();
    FileUtil.logFileUsage(cacheFile, "Override cache file", true);
  }

  FileCacheManager build() {
    // try to get cacheDir from system property or environment variable
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
      } catch (Throwable ex) {
        logger.debug(
            "Cannot get environment variable for cache directory, skip using cache", false);
        // In Boomi cloud, System.getenv is not allowed due to policy,
        // so we catch the exception and skip cache completely
        return this;
      }
    }

    if (cacheDirPath != null) {
      this.cacheDir = new File(cacheDirPath);
    } else {
      // use user home directory to store the cache file
      String homeDir = systemGetProperty("user.home");
      if (homeDir == null) {
        // use tmp dir if not exists.
        homeDir = systemGetProperty("java.io.tmpdir");
      } else {
        // Checking if home directory is writable.
        File homeFile = new File(homeDir);
        if (!homeFile.canWrite()) {
          logger.debug("Home directory not writeable, using tmpdir", false);
          homeDir = systemGetProperty("java.io.tmpdir");
        }
      }
      if (homeDir == null) {
        // if still home directory is null, no cache dir is set.
        return this;
      }
      if (Constants.getOS() == Constants.OS.WINDOWS) {
        this.cacheDir =
            new File(
                new File(new File(new File(homeDir, "AppData"), "Local"), "Snowflake"), "Caches");
      } else if (Constants.getOS() == Constants.OS.MAC) {
        this.cacheDir = new File(new File(new File(homeDir, "Library"), "Caches"), "Snowflake");
      } else {
        this.cacheDir = new File(new File(homeDir, ".cache"), "snowflake");
      }
    }

    if (!this.cacheDir.mkdirs() && !this.cacheDir.exists()) {
      logger.debug(
          "Cannot create the cache directory {}. Giving up.", this.cacheDir.getAbsolutePath());
      return this;
    }
    logger.debug("Verified Directory {}", this.cacheDir.getAbsolutePath());

    File cacheFileTmp = new File(this.cacheDir, this.baseCacheFileName).getAbsoluteFile();
    try {
      // create an empty file if not exists and return true.
      // If exists. the method returns false.
      // In this particular case, it doesn't matter as long as the file is
      // writable.
      if (cacheFileTmp.createNewFile()) {
        logger.debug("Successfully created a cache file {}", cacheFileTmp);
      } else {
        logger.debug("Cache file already exists {}", cacheFileTmp);
      }
      FileUtil.logFileUsage(cacheFileTmp, "Cache file creation", false);
      this.cacheFile = cacheFileTmp.getCanonicalFile();
      this.cacheLockFile =
          new File(this.cacheFile.getParentFile(), this.baseCacheFileName + ".lck");
    } catch (IOException | SecurityException ex) {
      logger.debug("Failed to touch the cache file. Ignored. {}", cacheFileTmp.getAbsoluteFile());
    }
    return this;
  }

  /** Reads the cache file. */
  JsonNode readCacheFile() {
    if (cacheFile == null || !this.checkCacheLockFile()) {
      // no cache or the cache is not valid.
      return null;
    }
    try {
      if (!cacheFile.exists()) {
        logger.debug("Cache file doesn't exists. File: {}", cacheFile);
        return null;
      }

      try (Reader reader =
          new InputStreamReader(new FileInputStream(cacheFile), DEFAULT_FILE_ENCODING)) {
        FileUtil.logFileUsage(cacheFile, "Read cache", false);
        return OBJECT_MAPPER.readTree(reader);
      }
    } catch (IOException ex) {
      logger.debug("Failed to read the cache file. No worry. File: {}, Err: {}", cacheFile, ex);
    }
    return null;
  }

  void writeCacheFile(JsonNode input) {
    logger.debug("Writing cache file. File: {}", cacheFile);
    if (cacheFile == null || !tryLockCacheFile()) {
      // no cache file or it failed to lock file
      logger.debug(
          "No cache file exists or failed to lock the file. Skipping writing the cache", false);
      return;
    }
    // NOTE: must unlock cache file
    try {
      if (input == null) {
        return;
      }
      try (Writer writer =
          new OutputStreamWriter(new FileOutputStream(cacheFile), DEFAULT_FILE_ENCODING)) {
        FileUtil.logFileUsage(cacheFile, "Write to cache", false);
        writer.write(input.toString());
      }
    } catch (IOException ex) {
      logger.debug("Failed to write the cache file. File: {}", cacheFile);
    } finally {
      if (!unlockCacheFile()) {
        logger.debug("Failed to unlock cache file", false);
      }
    }
  }

  void deleteCacheFile() {
    logger.debug("Deleting cache file. File: {}, lock file: {}", cacheFile, cacheLockFile);

    if (cacheFile == null) {
      return;
    }

    unlockCacheFile();
    if (!cacheFile.delete()) {
      logger.debug("Failed to delete the file: {}", cacheFile);
    }
  }

  /**
   * Tries to lock the cache file
   *
   * @return true if success or false
   */
  private boolean tryLockCacheFile() {
    int cnt = 0;
    boolean locked = false;
    while (cnt < 100 && !(locked = lockCacheFile())) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        // doesn't matter
      }
      ++cnt;
    }
    if (!locked) {
      logger.debug("Failed to lock the cache file.", false);
    }
    return locked;
  }

  /**
   * Lock cache file by creating a lock directory
   *
   * @return true if success or false
   */
  private boolean lockCacheFile() {
    return cacheLockFile.mkdirs();
  }

  /**
   * Unlock cache file by deleting a lock directory
   *
   * @return true if success or false
   */
  private boolean unlockCacheFile() {
    return cacheLockFile.delete();
  }

  private boolean checkCacheLockFile() {
    long currentTime = new Date().getTime();
    long cacheFileTs = fileCreationTime(cacheFile);

    if (!cacheLockFile.exists()
        && cacheFileTs > 0
        && currentTime - this.cacheExpirationInMilliseconds <= cacheFileTs) {
      logger.debug("No cache file lock directory exists and cache file is up to date.", false);
      return true;
    }

    long lockFileTs = fileCreationTime(cacheLockFile);
    if (lockFileTs < 0) {
      // failed to get the timestamp of lock directory
      return false;
    }
    if (lockFileTs < currentTime - this.cacheFileLockExpirationInMilliseconds) {
      // old lock file
      if (!cacheLockFile.delete()) {
        logger.debug("Failed to delete the directory. Dir: {}", cacheLockFile);
        return false;
      }
      logger.debug("Deleted the cache lock directory, because it was old.", false);
      return currentTime - this.cacheExpirationInMilliseconds <= cacheFileTs;
    }
    logger.debug("Failed to lock the file. Ignored.", false);
    return false;
  }

  /**
   * Gets file/dir creation time in epoch (ms)
   *
   * @return epoch time in ms
   */
  private static long fileCreationTime(File targetFile) {
    if (!targetFile.exists()) {
      logger.debug("File not exists. File: {}", targetFile);
      return -1;
    }
    try {
      Path cacheFileLockPath = Paths.get(targetFile.getAbsolutePath());
      BasicFileAttributes attr = Files.readAttributes(cacheFileLockPath, BasicFileAttributes.class);
      return attr.creationTime().toMillis();
    } catch (IOException ex) {
      logger.debug("Failed to get creation time. File/Dir: {}, Err: {}", targetFile, ex);
    }
    return -1;
  }

  String getCacheFilePath() {
    return cacheFile.getAbsolutePath();
  }
}
