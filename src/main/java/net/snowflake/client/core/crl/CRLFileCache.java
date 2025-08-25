package net.snowflake.client.core.crl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

class CRLFileCache {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLFileCache.class);

  private final Path cacheDir;
  private final Duration removalDelay;
  private final boolean enabled;
  private final Lock cacheLock = new ReentrantLock();

  CRLFileCache(Path cacheDir, Duration removalDelay, boolean enabled)
      throws SnowflakeSQLLoggedException {
    this.cacheDir = cacheDir;
    this.removalDelay = removalDelay;
    this.enabled = enabled;

    if (enabled) {
      try {
        boolean exists = Files.exists(cacheDir);
        if (!exists) {
          Files.createDirectories(cacheDir);
          logger.debug("Initialized CRL cache directory: {}", cacheDir);
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

  CRLCacheEntry get(String crlUrl) {
    if (!enabled) {
      return null;
    }

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

  void put(String crlUrl, X509CRL crl, Instant downloadTime) {
    if (!enabled) {
      return;
    }

    try {
      cacheLock.lock();
      Path crlFilePath = getCrlFilePath(crlUrl);

      Files.write(
          crlFilePath,
          crl.getEncoded(),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);

      Files.setLastModifiedTime(crlFilePath, FileTime.from(downloadTime));

      if (Constants.getOS() != Constants.OS.WINDOWS) {
        Files.setPosixFilePermissions(crlFilePath, PosixFilePermissions.fromString("rw-------"));
      }

      logger.debug("Updated disk cache for {}", crlUrl);
    } catch (Exception e) {
      logger.warn("Failed to write CRL to disk cache for {}: {}", crlUrl, e.getMessage());
    } finally {
      cacheLock.unlock();
    }
  }

  void cleanup() {
    if (!enabled) {
      return;
    }

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
              CRLCacheEntry crlEntry =
                  new CRLCacheEntry(crl, Files.getLastModifiedTime(filePath).toInstant());

              if (crlEntry.isCrlExpired(now) || crlEntry.isEvicted(now, removalDelay)) {
                Files.delete(filePath);
                removedCount++;
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
}
