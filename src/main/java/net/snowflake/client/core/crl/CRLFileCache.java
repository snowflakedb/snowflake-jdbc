package net.snowflake.client.core.crl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.core.Constants;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class CRLFileCache {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLFileCache.class);

  private final Path cacheDir;
  private final Duration removalDelay;
  private final boolean enabled;

  CRLFileCache(Path cacheDir, Duration removalDelay, boolean enabled) {
    this.cacheDir = cacheDir;
    this.removalDelay = removalDelay;
    this.enabled = enabled;

    if (enabled) {
      try {
        Files.createDirectories(cacheDir);

        if (Constants.getOS() != Constants.OS.WINDOWS) {
          Files.setPosixFilePermissions(cacheDir, PosixFilePermissions.fromString("rwx------"));
        }

        logger.debug("Initialized CRL cache directory: {}", cacheDir);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to create CRL cache directory " + cacheDir + ": " + e.getMessage(), e);
      }
    }
  }

  CRLCacheEntry get(String crlUrl) {
    if (!enabled) {
      return null;
    }

    try {
      Path crlFilePath = getCrlFilePath(crlUrl);
      if (Files.exists(crlFilePath)) {
        logger.debug("Found CRL on disk for {}", crlFilePath);

        BasicFileAttributes attrs = Files.readAttributes(crlFilePath, BasicFileAttributes.class);
        Instant downloadTime = attrs.lastModifiedTime().toInstant();

        byte[] crlBytes = Files.readAllBytes(crlFilePath);
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        X509CRL crl = (X509CRL) certFactory.generateCRL(new ByteArrayInputStream(crlBytes));

        return new CRLCacheEntry(crl, downloadTime);
      }
    } catch (Exception e) {
      logger.warn("Failed to read CRL from disk cache for {}: {}", crlUrl, e.getMessage());
    }

    return null;
  }

  void put(String crlUrl, X509CRL crl, Instant downloadTime) {
    if (!enabled) {
      return;
    }

    try {
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
      try (Stream<Path> stream = Files.list(cacheDir)) {
        for (Path filePath : stream.filter(Files::isRegularFile).collect(Collectors.toList())) {
          try {
            byte[] crlBytes = Files.readAllBytes(filePath);
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            X509CRL crl = (X509CRL) certFactory.generateCRL(new ByteArrayInputStream(crlBytes));

            if (crl.getNextUpdate() != null) {
              Instant expiryWithDelay = crl.getNextUpdate().toInstant().plus(removalDelay);
              if (expiryWithDelay.isBefore(now)) {
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
      }

      if (removedCount > 0) {
        logger.debug("Removed {} expired/corrupted files from disk CRL cache", removedCount);
      }
    } catch (Exception e) {
      logger.warn("Failed to cleanup disk CRL cache: {}", e.getMessage());
    }
  }

  private Path getCrlFilePath(String crlUrl) {
    try {
      String encodedUrl = URLEncoder.encode(crlUrl, StandardCharsets.UTF_8.toString());
      return cacheDir.resolve(encodedUrl);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
