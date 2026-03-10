package net.snowflake.client.internal.core;

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
import java.util.function.Supplier;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

class DefaultFileCacheManager implements FileCacheManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(DefaultFileCacheManager.class);

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  private static final Charset DEFAULT_FILE_ENCODING = StandardCharsets.UTF_8;

  private File cacheFile;
  private File cacheLockFile;
  private String baseCacheFileName;
  private final boolean onlyOwnerPermissions;
  private final long cacheFileLockExpirationInMilliseconds;

  DefaultFileCacheManager(
      File cacheFile,
      File cacheLockFile,
      String baseCacheFileName,
      boolean onlyOwnerPermissions,
      long cacheFileLockExpirationInMilliseconds) {
    this.cacheFile = cacheFile;
    this.cacheLockFile = cacheLockFile;
    this.baseCacheFileName = baseCacheFileName;
    this.onlyOwnerPermissions = onlyOwnerPermissions;
    this.cacheFileLockExpirationInMilliseconds = cacheFileLockExpirationInMilliseconds;
    logger.debug("Using cache file: {}", cacheFile.getAbsolutePath());
  }

  @Override
  public synchronized String getCacheFilePath() {
    return cacheFile.getAbsolutePath();
  }

  @Override
  public synchronized void overrideCacheFile(File newCacheFile) {
    if (!FileUtil.exists(newCacheFile)) {
      logger.debug("Cache file doesn't exist. File: {}", newCacheFile);
    }
    if (onlyOwnerPermissions) {
      FileUtil.handleWhenFilePermissionsWiderThanUserOnly(newCacheFile, "Override cache file");
      FileUtil.handleWhenParentDirectoryPermissionsWiderThanUserOnly(
          newCacheFile, "Override cache file");
    } else {
      FileUtil.logFileUsage(newCacheFile, "Override cache file", false);
    }
    this.cacheFile = newCacheFile;
    this.baseCacheFileName = newCacheFile.getName();
    this.cacheLockFile = new File(newCacheFile.getParentFile(), this.baseCacheFileName + ".lck");
  }

  @Override
  public synchronized <T> T withLock(Supplier<T> supplier) {
    if (cacheLockFile.exists()) {
      deleteCacheLockIfExpired();
    }

    if (!tryToLockCacheFile()) {
      logger.debug("Failed to lock the file. Skipping cache operation", false);
      return null;
    }
    try {
      return supplier.get();
    } finally {
      if (!unlockCacheFile()) {
        logger.debug("Failed to unlock cache file", false);
      }
    }
  }

  @Override
  public synchronized JsonNode readCacheFile() {
    try {
      if (!cacheFile.exists()) {
        logger.debug("Cache file doesn't exist. Ignoring read. File: {}", cacheFile);
        return null;
      }

      try (Reader reader =
          new InputStreamReader(new FileInputStream(cacheFile), DEFAULT_FILE_ENCODING)) {

        if (onlyOwnerPermissions) {
          FileUtil.handleWhenFilePermissionsWiderThanUserOnly(cacheFile, "Read cache");
          FileUtil.handleWhenParentDirectoryPermissionsWiderThanUserOnly(cacheFile, "Read cache");
          FileUtil.throwWhenOwnerDifferentThanCurrentUser(cacheFile, "Read cache");
        } else {
          FileUtil.logFileUsage(cacheFile, "Read cache", false);
        }
        return OBJECT_MAPPER.readTree(reader);
      }
    } catch (IOException ex) {
      logger.debug("Failed to read the cache file. No worry. File: {}, Err: {}", cacheFile, ex);
    }
    return null;
  }

  @Override
  public synchronized void writeCacheFile(JsonNode input) {
    logger.debug("Writing cache file. File: {}", cacheFile);
    try {
      if (input == null || !cacheFile.exists()) {
        logger.debug(
            "Cache file doesn't exist or input is null. Ignoring write. File: {}", cacheFile);
        return;
      }
      try (Writer writer =
          new OutputStreamWriter(new FileOutputStream(cacheFile), DEFAULT_FILE_ENCODING)) {
        if (onlyOwnerPermissions) {
          FileUtil.handleWhenFilePermissionsWiderThanUserOnly(cacheFile, "Write to cache");
          FileUtil.handleWhenParentDirectoryPermissionsWiderThanUserOnly(
              cacheFile, "Write to cache");
        } else {
          FileUtil.logFileUsage(cacheFile, "Write to cache", false);
        }
        writer.write(input.toString());
      }
    } catch (IOException ex) {
      logger.debug("Failed to write the cache file. File: {}", cacheFile);
    }
  }

  @Override
  public synchronized void deleteCacheFile() {
    logger.debug("Deleting cache file. File: {}, lock file: {}", cacheFile, cacheLockFile);
    unlockCacheFile();
    if (!cacheFile.delete()) {
      logger.debug("Failed to delete the file: {}", cacheFile);
    }
  }

  private synchronized boolean tryToLockCacheFile() {
    int cnt = 0;
    boolean locked = false;
    while (cnt < 5 && !(locked = lockCacheFile())) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
        // doesn't matter
      }
      ++cnt;
    }
    if (!locked) {
      deleteCacheLockIfExpired();
      if (!lockCacheFile()) {
        logger.debug("Failed to lock the cache file.", false);
      }
    }
    return locked;
  }

  private synchronized void deleteCacheLockIfExpired() {
    long currentTime = new Date().getTime();
    long lockFileTs = fileCreationTime(cacheLockFile);
    if (lockFileTs < 0) {
      logger.debug("Failed to get the timestamp of lock directory");
    } else if (lockFileTs < currentTime - this.cacheFileLockExpirationInMilliseconds) {
      try {
        if (!cacheLockFile.delete()) {
          logger.debug("Failed to delete the directory. Dir: {}", cacheLockFile);
        } else {
          logger.debug("Deleted expired cache lock directory.", false);
        }
      } catch (Exception e) {
        logger.debug(
            "Failed to delete the directory. Dir: {}, Error: {}", cacheLockFile, e.getMessage());
      }
    }
  }

  private static synchronized long fileCreationTime(File targetFile) {
    if (!FileUtil.exists(targetFile)) {
      logger.debug("File does not exist. File: {}", targetFile);
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

  private synchronized boolean lockCacheFile() {
    return cacheLockFile.mkdirs();
  }

  private synchronized boolean unlockCacheFile() {
    return cacheLockFile.delete();
  }
}
