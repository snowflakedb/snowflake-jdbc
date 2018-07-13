/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;

class FileCacheManager
{
  private static final
  SFLogger LOGGER = SFLoggerFactory.getLogger(FileCacheManager.class);

  /**
   * Object mapper for JSON encoding and decoding
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Charset DEFAULT_FILE_ENCODING = Charset.forName("UTF-8");

  private String cacheDirectoryEnvironmentVariable;
  private String baseCacheFileName;
  private long cacheExpiration;
  private long cacheFileLockExpiration;

  private File cacheFile;
  private File cacheLockFile;

  private File cacheDir;

  private FileCacheManager()
  {
  }

  static FileCacheManager builder()
  {
    return new FileCacheManager();
  }

  FileCacheManager setCacheDirectoryEnvironmentVariable(String cacheDirectoryEnvironmentVariable)
  {
    this.cacheDirectoryEnvironmentVariable = cacheDirectoryEnvironmentVariable;
    return this;
  }

  FileCacheManager setBaseCacheFileName(String baseCacheFileName)
  {
    this.baseCacheFileName = baseCacheFileName;
    return this;
  }

  FileCacheManager setCacheExpiration(long cacheExpiration)
  {
    this.cacheExpiration = cacheExpiration;
    return this;
  }

  FileCacheManager setCacheFileLockExpiration(long cacheFileLockExpiration)
  {
    this.cacheFileLockExpiration = cacheFileLockExpiration;
    return this;
  }

  /**
   * Override the cache file.
   *
   * @param newCacheFile a file object to override the default one.
   */
  void overrideCacheFile(File newCacheFile)
  {
    this.cacheFile = newCacheFile;
    this.cacheDir = newCacheFile.getParentFile();
    this.baseCacheFileName = newCacheFile.getName();
  }

  FileCacheManager build()
  {
    String cacheDir = System.getenv(this.cacheDirectoryEnvironmentVariable);
    if (cacheDir != null)
    {
      this.cacheDir = new File(cacheDir);
    }
    else
    {
      // use user home directory to store the cache file
      String homeDir = System.getProperty("user.home");
      if (homeDir == null)
      {
        // use tmp dir if not exists.
        homeDir = System.getProperty("java.io.tmpdir");
      }
      if (Constants.getOS() == Constants.OS.WINDOWS)
      {
        this.cacheDir = new File(
            new File(new File(new File(homeDir,
                "AppData"), "Local"), "Snowflake"), "Caches");
      }
      else if (Constants.getOS() == Constants.OS.MAC)
      {
        this.cacheDir = new File(new File(new File(homeDir,
            "Library"), "Caches"), "Snowflake");
      }
      else
      {
        this.cacheDir = new File(new File(homeDir, ".cache"), "snowflake");
      }
    }
    if (!this.cacheDir.exists() && !this.cacheDir.mkdirs())
    {
      throw new RuntimeException(
          String.format(
              "Failed to locate or create the cache directory: %s", this.cacheDir)
      );
    }

    File cacheFileTmp = new File(
        this.cacheDir, this.baseCacheFileName).getAbsoluteFile();
    try
    {
      // create an empty file if not exists and return true.
      // If exists. the method returns false.
      // In this particular case, it doesn't matter as long as the file is
      // writable.
      cacheFileTmp.createNewFile();
      this.cacheFile = cacheFileTmp.getCanonicalFile();
      this.cacheLockFile = new File(
          this.cacheFile.getParentFile(), this.baseCacheFileName + ".lck");
    }
    catch (IOException | SecurityException ex)
    {
      throw new RuntimeException(
          String.format(
              "Failed to touch the cache file: %s",
              cacheFileTmp.getAbsoluteFile())
      );
    }
    return this;
  }

  /**
   * Reads the cache file.
   */
  JsonNode readCacheFile()
  {
    // File cacheFile = fileCacheManager.getCacheFile();
    // File cacheFileLock = fileCacheManager.getCacheLockFile();
    if (cacheFile == null || !this.checkCacheLockFile())
    {
      // no cache or the cache is not valid.
      return null;
    }
    try
    {
      if (!cacheFile.exists())
      {
        LOGGER.debug(
            "Cache file doesn't exists. File: {}", cacheFile);
        return null;
      }

      try (Reader reader = new InputStreamReader(
          new FileInputStream(cacheFile), DEFAULT_FILE_ENCODING))
      {
        return OBJECT_MAPPER.readTree(reader);
      }
    }
    catch (IOException ex)
    {
      LOGGER.debug(
          "Failed to read the cache file. No worry. File: {}, Err: {}",
          cacheFile, ex);
    }
    return null;
  }

  void writeCacheFile(JsonNode input)
  {
    LOGGER.debug("Writing OCSP response cache file. File={}", cacheFile);
    if (cacheFile == null || !tryLockCacheFile())
    {
      // no cache file or it failed to lock file
      return;
    }
    // NOTE: must unlock cache file
    try
    {
      if (input == null)
      {
        return;
      }
      try (Writer writer = new OutputStreamWriter(
          new FileOutputStream(cacheFile), DEFAULT_FILE_ENCODING))
      {
        writer.write(input.toString());
      }
    }
    catch (IOException ex)
    {
      LOGGER.debug(
          "Failed to write the OCSP response cache file. File: {}",
          cacheFile);
    }
    finally
    {
      if (!unlockCacheFile())
      {
        LOGGER.debug("Failed to unlock cache file");
      }
    }
  }

  /**
   * Tries to lock the cache file
   *
   * @return true if success or false
   */
  private boolean tryLockCacheFile()
  {
    int cnt = 0;
    boolean locked = false;
    while (cnt < 100 && !(locked = lockCacheFile()))
    {
      try
      {
        Thread.sleep(100);
      }
      catch (InterruptedException ex)
      {
        // doesn't matter
      }
      ++cnt;
    }
    if (!locked)
    {
      LOGGER.debug("Failed to lock the OCSP response cache file.");
    }
    return locked;
  }

  /**
   * Lock cache file by creating a lock directory
   *
   * @return true if success or false
   */
  private boolean lockCacheFile()
  {
    return cacheLockFile.mkdirs();
  }

  /**
   * Unlock cache file by deleting a lock directory
   *
   * @return true if success or false
   */
  private boolean unlockCacheFile()
  {
    return cacheLockFile.delete();
  }

  private boolean checkCacheLockFile()
  {
    long currentTime = new Date().getTime();
    long cacheFileTs = fileCreationTime(cacheFile);

    if (!cacheLockFile.exists() && cacheFileTs > 0 && currentTime -
        this.cacheExpiration <= cacheFileTs)
    {
      LOGGER.debug("No cache file lock directory exists and cache file is up to date.");
      return true;
    }

    long lockFileTs = fileCreationTime(cacheLockFile);
    if (lockFileTs < 0)
    {
      // failed to get the timestamp of lock directory
      return false;
    }
    if (lockFileTs < currentTime - this.cacheFileLockExpiration)
    {
      // old lock file
      if (!cacheLockFile.delete())
      {
        LOGGER.debug(
            "Failed to delete the directory. Dir: {}",
            cacheLockFile);
        return false;
      }
      LOGGER.debug("Deleted the cache lock directory, because it was old.");
      return currentTime - this.cacheExpiration <= cacheFileTs;
    }
    LOGGER.debug("Failed to lock the file. Ignored.");
    return false;
  }

  /**
   * Gets file/dir creation time in epoch (ms)
   *
   * @return epoch time in ms
   */
  private static long fileCreationTime(File targetFile)
  {
    if (!targetFile.exists())
    {
      LOGGER.debug("File not exists. File: {}", targetFile);
      return -1;
    }
    try
    {
      Path cacheFileLockPath = Paths.get(targetFile.getAbsolutePath());
      BasicFileAttributes attr = Files.readAttributes(
          cacheFileLockPath, BasicFileAttributes.class);
      return attr.creationTime().toMillis();
    }
    catch (IOException ex)
    {
      LOGGER.debug(
          "Failed to get creation time. File/Dir: {}, Err: {}",
          targetFile, ex);
    }
    return -1;
  }
}
