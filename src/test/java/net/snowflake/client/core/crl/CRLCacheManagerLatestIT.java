package net.snowflake.client.core.crl;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.stream.Stream;
import net.snowflake.client.annotations.DontRunOnWindows;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(TestTags.CORE)
public class CRLCacheManagerLatestIT {

  private static final String TEST_CRL_URL = "http://snowflake.com/test.crl";
  private static final String TEST_CRL_URL_2 = "http://snowflake.com/test2.crl";
  private static final CertificateGeneratorUtil certGen = new CertificateGeneratorUtil();

  @TempDir private Path tempCacheDir;

  private CRLCacheManager cacheManager;
  private CRLFileCache crlFileCache;
  private CRLInMemoryCache crlInMemoryCache;
  private X509CRL testCRL;
  private X509CRL testCRL2;
  private CRLCacheEntry cacheEntry;
  private Instant downloadTime;

  @BeforeEach
  void setUp() throws Exception {
    downloadTime = Instant.now();
    testCRL = createTestCrl();
    testCRL2 = createTestCrl();
    crlInMemoryCache = new CRLInMemoryCache(Duration.ofSeconds(10));
    crlFileCache = new CRLFileCache(tempCacheDir, Duration.ofSeconds(10));
    cacheManager =
        new CRLCacheManager(
            crlInMemoryCache, crlFileCache, Duration.ofSeconds(10), Duration.ofSeconds(10));
    cacheEntry = new CRLCacheEntry(testCRL, downloadTime);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (cacheManager != null) {
      cleanupTempFiles();
    }
  }

  @Test
  void testFileCacheStoreAndRetrieve() throws Exception {
    crlFileCache.put(TEST_CRL_URL, cacheEntry);

    CRLCacheEntry entry = crlFileCache.get(TEST_CRL_URL);

    assertNotNull(entry);
    assertEquals(testCRL.getEncoded().length, entry.getCrl().getEncoded().length);
    assertTrue(
        Math.abs(downloadTime.toEpochMilli() - entry.getDownloadTime().toEpochMilli()) < 1000);
  }

  @Test
  @DontRunOnWindows
  void testFileCacheFilePermissions() throws Exception {
    crlFileCache.put(TEST_CRL_URL, cacheEntry);
    try (Stream<Path> files = Files.list(tempCacheDir)) {
      Path crlFile = files.filter(Files::isRegularFile).findFirst().orElse(null);
      assertNotNull(crlFile);
      String permissions = PosixFilePermissions.toString(Files.getPosixFilePermissions(crlFile));
      assertEquals("rw-------", permissions);
    }
  }

  @Test
  void testFileCacheCorruptedFileHandling() throws Exception {
    crlFileCache.put(TEST_CRL_URL, cacheEntry);
    Path corruptedFile = tempCacheDir.resolve("corrupted.crl");
    Files.write(corruptedFile, "This is not a valid CRL".getBytes());

    assertEquals(2, countFilesInCache());

    crlFileCache.cleanup();

    assertEquals(1, countFilesInCache());
    assertFalse(Files.exists(corruptedFile));
    assertNotNull(crlFileCache.get(TEST_CRL_URL));
  }

  @Test
  void testFileCacheExpiredCrlRemoval() throws Exception {
    CRLCacheEntry expiredEntry =
        new CRLCacheEntry(createExpiredCrl(), downloadTime.minus(1, ChronoUnit.DAYS));
    crlFileCache.put(TEST_CRL_URL, expiredEntry);
    crlFileCache.put(TEST_CRL_URL_2, cacheEntry);

    assertEquals(2, countFilesInCache());

    crlFileCache.cleanup();

    assertEquals(1, countFilesInCache());
    assertNull(crlFileCache.get(TEST_CRL_URL));
    assertNotNull(crlFileCache.get(TEST_CRL_URL_2));
  }

  @Test
  void testFileCacheRemovalDelay() throws Exception {
    Duration removalDelay = Duration.ofHours(1);
    CRLFileCache delayedCache = new CRLFileCache(tempCacheDir, removalDelay);
    CRLCacheEntry oldCacheEntry =
        new CRLCacheEntry(testCRL, downloadTime.minus(30, ChronoUnit.MINUTES));
    delayedCache.put(TEST_CRL_URL, oldCacheEntry);

    assertEquals(1, countFilesInCache());

    delayedCache.cleanup();

    assertEquals(1, countFilesInCache());
    assertNotNull(delayedCache.get(TEST_CRL_URL));
  }

  @Test
  void testFileCachePromotionToMemoryCache() throws Exception {
    crlFileCache.put(TEST_CRL_URL, cacheEntry);

    assertNull(crlInMemoryCache.get(TEST_CRL_URL));
    assertNotNull(crlFileCache.get(TEST_CRL_URL));

    // should promote to memory cache
    cacheManager.get(TEST_CRL_URL);

    assertNotNull(crlFileCache.get(TEST_CRL_URL));
    assertNotNull(crlInMemoryCache.get(TEST_CRL_URL));
  }

  @Test
  void testCacheManagerPeriodicCleanup() throws Exception {
    CRLCacheManager managerWithCleanup =
        CRLCacheManager.build(
            true, true, tempCacheDir, Duration.ofMillis(100), Duration.ofMillis(10));
    managerWithCleanup.put(TEST_CRL_URL, testCRL, downloadTime.minus(200, ChronoUnit.MILLIS));
    managerWithCleanup.put(TEST_CRL_URL_2, testCRL2, downloadTime);

    assertNotNull(managerWithCleanup.get(TEST_CRL_URL));
    assertNotNull(managerWithCleanup.get(TEST_CRL_URL_2));

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              assertNull(managerWithCleanup.get(TEST_CRL_URL));
              assertNull(managerWithCleanup.get(TEST_CRL_URL_2));
            });
  }

  @Test
  void testCrlUpdateScenario() throws Exception {
    Instant firstDownload = downloadTime.minus(1, ChronoUnit.HOURS);
    Instant secondDownload = downloadTime;
    cacheManager.put(TEST_CRL_URL, testCRL, firstDownload);
    cacheManager.put(TEST_CRL_URL, testCRL2, secondDownload);

    CRLCacheEntry entry = cacheManager.get(TEST_CRL_URL);

    assertNotNull(entry);
    assertEquals(testCRL2.getEncoded().length, entry.getCrl().getEncoded().length);
    assertEquals(secondDownload, entry.getDownloadTime());
  }

  private X509CRL createTestCrl() throws Exception {
    Date futureDate = Date.from(downloadTime.plus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(futureDate);
  }

  private X509CRL createExpiredCrl() throws Exception {
    Date pastDate = Date.from(downloadTime.minus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(pastDate);
  }

  private void cleanupTempFiles() throws Exception {
    if (Files.exists(tempCacheDir)) {
      try (Stream<Path> paths = Files.walk(tempCacheDir)) {
        paths
            .filter(Files::isRegularFile)
            .forEach(
                file -> {
                  try {
                    Files.deleteIfExists(file);
                  } catch (IOException ignored) {
                  }
                });
      }
    }
  }

  private int countFilesInCache() throws IOException {
    try (Stream<Path> files = Files.list(tempCacheDir)) {
      return (int) files.filter(Files::isRegularFile).count();
    }
  }
}
