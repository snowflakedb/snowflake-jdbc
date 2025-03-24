package net.snowflake.client.core;

import static net.snowflake.client.core.FileUtil.isWritable;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;
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
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Date;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private long cacheFileLockExpirationInMilliseconds;

  private File cacheFile;
  private File cacheLockFile;

  private File cacheDir;

  private boolean onlyOwnerPermissions = true;

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

  FileCacheManager setCacheFileLockExpirationInSeconds(long cacheFileLockExpirationInSeconds) {
    this.cacheFileLockExpirationInMilliseconds = cacheFileLockExpirationInSeconds * 1000;
    return this;
  }

  FileCacheManager setOnlyOwnerPermissions(boolean onlyOwnerPermissions) {
    this.onlyOwnerPermissions = onlyOwnerPermissions;
    return this;
  }

  synchronized String getCacheFilePath() {
    return cacheFile.getAbsolutePath();
  }

  /**
   * Override the cache file.
   *
   * @param newCacheFile a file object to override the default one.
   */
  synchronized void overrideCacheFile(File newCacheFile) {
    if (!FileUtil.exists(newCacheFile)) {
      logger.debug("Cache file doesn't exist. File: {}", newCacheFile);
    }
    if (onlyOwnerPermissions) {
      FileUtil.handleWhenFilePermissionsWiderThanUserOnly(newCacheFile, "Override cache file");
      FileUtil.handleWhenParentDirectoryPermissionsWiderThanUserOnly(
          newCacheFile, "Override cache file");
    } else {
      FileUtil.logFileUsage(cacheFile, "Override cache file", false);
    }
    this.cacheFile = newCacheFile;
    this.cacheDir = newCacheFile.getParentFile();
    this.baseCacheFileName = newCacheFile.getName();
  }

  synchronized FileCacheManager build() {
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
      this.cacheDir = getDefaultCacheDir();
    }
    if (cacheDir == null) {
      return this;
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
        return this;
      }
    }
    if (!this.cacheDir.exists()) {
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
      this.cacheFile = cacheFileTmp.getCanonicalFile();
      this.cacheLockFile =
          new File(this.cacheFile.getParentFile(), this.baseCacheFileName + ".lck");
    } catch (IOException | SecurityException ex) {
      logger.info(
          "Failed to touch the cache file: {}. Ignored. {}",
          ex.getMessage(),
          cacheFileTmp.getAbsoluteFile());
    }
    return this;
  }

  static File getDefaultCacheDir() {
    if (Constants.getOS() == Constants.OS.LINUX) {
      String xdgCacheHome = getXdgCacheHome();
      if (xdgCacheHome != null) {
        return new File(xdgCacheHome, "snowflake");
      }
    }

    String homeDir = getHomeDirProperty();
    if (homeDir == null) {
      // if still home directory is null, no cache dir is set.
      return null;
    }
    if (Constants.getOS() == Constants.OS.WINDOWS) {
      return new File(
          new File(new File(new File(homeDir, "AppData"), "Local"), "Snowflake"), "Caches");
    } else if (Constants.getOS() == Constants.OS.MAC) {
      return new File(new File(new File(homeDir, "Library"), "Caches"), "Snowflake");
    } else {
      return new File(new File(homeDir, ".cache"), "snowflake");
    }
  }

  private static String getXdgCacheHome() {
    String xdgCacheHome = systemGetEnv("XDG_CACHE_HOME");
    if (xdgCacheHome != null && isWritable(xdgCacheHome)) {
      return xdgCacheHome;
    }
    return null;
  }

  private static String getHomeDirProperty() {
    String homeDir = systemGetProperty("user.home");
    if (homeDir != null && isWritable(homeDir)) {
      return homeDir;
    }
    return null;
  }

  synchronized <T> T withLock(Supplier<T> supplier) {
    if (cacheFile == null) {
      logger.error("No cache file assigned", false);
      return null;
    }
    if (cacheLockFile == null) {
      logger.error("No cache lock file assigned", false);
      return null;
    } else if (cacheLockFile.exists()) {
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

  /** Reads the cache file. */
  synchronized JsonNode readCacheFile() {
    try {
      if (!FileUtil.exists(cacheFile)) {
        logger.debug("Cache file doesn't exist. File: {}", cacheFile);
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

  synchronized void writeCacheFile(JsonNode input) {
    logger.debug("Writing cache file. File: {}", cacheFile);
    try {
      if (input == null) {
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

  synchronized void deleteCacheFile() {
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
      // old lock file
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

  /**
   * Gets file/dir creation time in epoch (ms)
   *
   * @return epoch time in ms
   */
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

  /**
   * Lock cache file by creating a lock directory
   *
   * @return true if success or false
   */
  private synchronized boolean lockCacheFile() {
    return cacheLockFile.mkdirs();
  }

  /**
   * Unlock cache file by deleting a lock directory
   *
   * @return true if success or false
   */
  private synchronized boolean unlockCacheFile() {
    return cacheLockFile.delete();
  }
}
