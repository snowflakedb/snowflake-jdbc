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

  @BeforeEach
  void setUp() throws Exception {
    testCRL = createTestCrl();
    testCRL2 = createTestCrl();

    crlInMemoryCache = new CRLInMemoryCache(Duration.ofSeconds(10), true);
    crlFileCache = new CRLFileCache(tempCacheDir, Duration.ofSeconds(10), true);
    cacheManager = new CRLCacheManager(crlInMemoryCache, crlFileCache, Duration.ofSeconds(10));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (cacheManager != null) {
      cleanupTempFiles();
    }
  }

  @Test
  void testFileCacheStoreAndRetrieve() throws Exception {
    Instant downloadTime = Instant.now();
    crlFileCache.put(TEST_CRL_URL, testCRL, downloadTime);

    CRLCacheEntry entry = crlFileCache.get(TEST_CRL_URL);

    assertNotNull(entry);
    assertEquals(testCRL.getEncoded().length, entry.getCrl().getEncoded().length);
    assertTrue(
        Math.abs(downloadTime.toEpochMilli() - entry.getDownloadTime().toEpochMilli()) < 1000);
  }

  @Test
  @DontRunOnWindows
  void testFileCacheFilePermissions() throws Exception {
    crlFileCache.put(TEST_CRL_URL, testCRL, Instant.now());
    try (Stream<Path> files = Files.list(tempCacheDir)) {
      Path crlFile = files.filter(Files::isRegularFile).findFirst().orElse(null);
      assertNotNull(crlFile);
      String permissions = PosixFilePermissions.toString(Files.getPosixFilePermissions(crlFile));
      assertEquals("rw-------", permissions);
    }
  }

  @Test
  void testFileCacheCorruptedFileHandling() throws Exception {
    crlFileCache.put(TEST_CRL_URL, testCRL, Instant.now());
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
    X509CRL expiredCrl = createExpiredCrl();
    crlFileCache.put(TEST_CRL_URL, expiredCrl, Instant.now().minus(2, ChronoUnit.DAYS));
    crlFileCache.put(TEST_CRL_URL_2, testCRL, Instant.now());

    assertEquals(2, countFilesInCache());

    crlFileCache.cleanup();

    assertEquals(1, countFilesInCache());
    assertNull(crlFileCache.get(TEST_CRL_URL));
    assertNotNull(crlFileCache.get(TEST_CRL_URL_2));
  }

  @Test
  void testFileCacheRemovalDelay() throws Exception {
    Duration removalDelay = Duration.ofHours(1);
    CRLFileCache delayedCache = new CRLFileCache(tempCacheDir, removalDelay, true);
    delayedCache.put(TEST_CRL_URL, testCRL, Instant.now().minus(30, ChronoUnit.MINUTES));

    assertEquals(1, countFilesInCache());

    delayedCache.cleanup();

    assertEquals(1, countFilesInCache());
    assertNotNull(delayedCache.get(TEST_CRL_URL));
  }

  @Test
  void testFileCachePromotionToMemoryCache() throws Exception {
    Instant downloadTime = Instant.now();
    crlFileCache.put(TEST_CRL_URL, testCRL, downloadTime);

    assertNull(crlInMemoryCache.get(TEST_CRL_URL));
    assertNotNull(crlFileCache.get(TEST_CRL_URL));

    // should promote to memory cache
    cacheManager.get(TEST_CRL_URL);

    assertNotNull(crlFileCache.get(TEST_CRL_URL));
    assertNotNull(crlInMemoryCache.get(TEST_CRL_URL));
  }

  @Test
  void testCacheManagerPeriodicCleanup() throws Exception {
    CRLValidationConfig config =
        CRLValidationConfig.builder()
            .inMemoryCacheEnabled(true)
            .onDiskCacheEnabled(true)
            .onDiskCacheDir(tempCacheDir)
            .cacheValidityTime(Duration.ofMillis(10))
            .onDiskCacheRemovalDelay(Duration.ofMillis(100))
            .build();

    CRLCacheManager managerWithCleanup = CRLCacheManager.fromConfig(config);
    managerWithCleanup.put(TEST_CRL_URL, testCRL, Instant.now().minus(200, ChronoUnit.MILLIS));
    managerWithCleanup.put(TEST_CRL_URL_2, testCRL2, Instant.now());

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
    Instant firstDownload = Instant.now().minus(1, ChronoUnit.HOURS);
    Instant secondDownload = Instant.now();
    cacheManager.put(TEST_CRL_URL, testCRL, firstDownload);
    cacheManager.put(TEST_CRL_URL, testCRL2, secondDownload);

    CRLCacheEntry entry = cacheManager.get(TEST_CRL_URL);

    assertNotNull(entry);
    assertEquals(testCRL2.getEncoded().length, entry.getCrl().getEncoded().length);
    assertEquals(secondDownload, entry.getDownloadTime());
  }

  private X509CRL createTestCrl() throws Exception {
    Date futureDate = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(futureDate);
  }

  private X509CRL createExpiredCrl() throws Exception {
    Date pastDate = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
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
