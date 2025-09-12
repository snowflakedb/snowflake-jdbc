package net.snowflake.client.core.crl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.core.Constants;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

class CRLFileCache implements CRLCache {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLFileCache.class);

  private final Path cacheDir;
  private final Duration removalDelay;
  private final Lock cacheLock = new ReentrantLock();

  CRLFileCache(Path cacheDir, Duration removalDelay) throws SnowflakeSQLLoggedException {
    this.cacheDir = cacheDir;
    this.removalDelay = removalDelay;

    ensureCacheDirectoryExists(cacheDir);
  }

  public CRLCacheEntry get(String crlUrl) {
    try {
      cacheLock.lock();
      Path crlFilePath = getCrlFilePath(crlUrl);
      if (Files.exists(crlFilePath)) {
        logger.debug("Found CRL on disk for {}", crlFilePath);

        BasicFileAttributes attrs = Files.readAttributes(crlFilePath, BasicFileAttributes.class);
        Instant downloadTime = attrs.lastModifiedTime().toInstant();

        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        try (InputStream crlBytes = Files.newInputStream(crlFilePath)) {
          X509CRL crl = (X509CRL) certFactory.generateCRL(crlBytes);
          return new CRLCacheEntry(crl, downloadTime);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to read CRL from disk cache for {}: {}", crlUrl, e.getMessage());
    } finally {
      cacheLock.unlock();
    }

    return null;
  }

  public void put(String crlUrl, CRLCacheEntry entry) {
    try {
      cacheLock.lock();
      Path crlFilePath = getCrlFilePath(crlUrl);

      if (Constants.getOS() != Constants.OS.WINDOWS) {
        Files.deleteIfExists(crlFilePath);
        Files.createFile(
            crlFilePath,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.write(crlFilePath, entry.getCrl().getEncoded());
      } else {
        Files.write(crlFilePath, entry.getCrl().getEncoded());
      }

      Files.setLastModifiedTime(crlFilePath, FileTime.from(entry.getDownloadTime()));

      logger.debug("Updated disk cache for {}", crlUrl);
    } catch (Exception e) {
      logger.warn("Failed to write CRL to disk cache for {}: {}", crlUrl, e.getMessage());
    } finally {
      cacheLock.unlock();
    }
  }

  public void cleanup() {
    Instant now = Instant.now();
    logger.debug("Cleaning up on-disk CRL cache at {}", now);

    try {
      if (!Files.exists(cacheDir)) {
        return;
      }

      int removedCount = 0;
      try (Stream<Path> files = Files.list(cacheDir)) {
        cacheLock.lock();
        for (Path filePath : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
          try {
            try (InputStream crlBytes = Files.newInputStream(filePath)) {
              CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
              X509CRL crl = (X509CRL) certFactory.generateCRL(crlBytes);
              CRLCacheEntry entry =
                  new CRLCacheEntry(crl, Files.getLastModifiedTime(filePath).toInstant());

              boolean expired = entry.isCrlExpired(now);
              boolean evicted = entry.isEvicted(now, removalDelay);
              if (expired || evicted) {
                Files.delete(filePath);
                removedCount++;
                logger.debug(
                    "Removing file based CRL cache entry for {}: expired={}, evicted={}",
                    filePath,
                    expired,
                    evicted);
              }
            }
          } catch (IOException | CRLException | CertificateException e) {
            // If we can't parse the file, it's probably corrupted - remove it
            try {
              Files.delete(filePath);
              removedCount++;
            } catch (IOException deleteError) {
              logger.warn(
                  "Failed to delete corrupted CRL file {}: {}", filePath, deleteError.getMessage());
            }
          }
        }
      } finally {
        cacheLock.unlock();
      }

      if (removedCount > 0) {
        logger.debug("Removed {} expired/corrupted files from disk CRL cache", removedCount);
      }
    } catch (Exception e) {
      logger.warn("Failed to cleanup disk CRL cache: {}", e.getMessage());
    }
  }

  private Path getCrlFilePath(String crlUrl) throws UnsupportedEncodingException {
    String encodedUrl = URLEncoder.encode(crlUrl, StandardCharsets.UTF_8.toString());
    return cacheDir.resolve(encodedUrl);
  }

  private static boolean ownerOnlyPermissions(Path cacheDir) throws IOException {
    return Files.getPosixFilePermissions(cacheDir)
        .equals(PosixFilePermissions.fromString("rwx------"));
  }

  private static void ensureCacheDirectoryExists(Path cacheDir) throws SnowflakeSQLLoggedException {
    try {
      boolean exists = Files.exists(cacheDir);
      if (!exists) {
        if (Constants.getOS() != Constants.OS.WINDOWS) {
          Files.createDirectories(
              cacheDir,
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
          logger.debug("Initialized CRL cache directory: {}", cacheDir);
        } else {
          Files.createDirectories(cacheDir);
        }
      }

      if (Constants.getOS() != Constants.OS.WINDOWS && !ownerOnlyPermissions(cacheDir)) {
        Files.setPosixFilePermissions(cacheDir, PosixFilePermissions.fromString("rwx------"));
        logger.debug("Set CRL cache directory permissions to 'rwx------");
      }
    } catch (Exception e) {
      throw new SnowflakeSQLLoggedException(
          null, null, SqlState.INTERNAL_ERROR, "Failed to create CRL cache directory", e);
    }
  }
}
