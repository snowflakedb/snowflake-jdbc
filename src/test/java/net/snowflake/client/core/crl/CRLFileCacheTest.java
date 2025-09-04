package net.snowflake.client.core.crl;

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
import net.snowflake.client.core.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(TestTags.CORE)
class CRLFileCacheTest {
  private static final String TEST_CRL_URL = "http://snowflake.com/test.crl";
  private static final String TEST_CRL_URL_2 = "http://snowflake.com/test2.crl";
  private static CertificateGeneratorUtil certGen;

  @TempDir private Path tempCacheDir;

  private CRLFileCache cache;
  private X509CRL testCRL;
  private X509CRL testCRL2;
  private Instant downloadTime;
  private CRLCacheEntry cacheEntry;

  @BeforeAll
  static void setUpClass() {
    certGen = new CertificateGeneratorUtil();
  }

  @BeforeEach
  void setUp() throws Exception {
    downloadTime = Instant.now();
    testCRL = createValidCrl();
    testCRL2 = createValidCrl();
    cache = new CRLFileCache(tempCacheDir, Duration.ofMinutes(10));
    cacheEntry = new CRLCacheEntry(testCRL, downloadTime);
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanupTempFiles();
  }

  @Test
  void testEnabledCacheCreatesDirectory() throws Exception {
    Path newCacheDir = tempCacheDir.resolve("new-cache");
    assertFalse(Files.exists(newCacheDir));

    cache = new CRLFileCache(newCacheDir, Duration.ofMinutes(10));

    assertTrue(Files.exists(newCacheDir));
    assertTrue(Files.isDirectory(newCacheDir));
    if ((Constants.getOS() != Constants.OS.WINDOWS)) {
      String permissions =
          PosixFilePermissions.toString(Files.getPosixFilePermissions(newCacheDir));
      assertEquals("rwx------", permissions);
    }
  }

  @Test
  void testBasicPutAndGet() throws Exception {
    cache.put(TEST_CRL_URL, cacheEntry);

    assertEquals(1, countFilesInCache());

    CRLCacheEntry retrieved = cache.get(TEST_CRL_URL);
    assertNotNull(retrieved);
    assertEquals(testCRL.getEncoded().length, retrieved.getCrl().getEncoded().length);
    assertTrue(
        Math.abs(downloadTime.toEpochMilli() - retrieved.getDownloadTime().toEpochMilli()) < 1000);
  }

  @Test
  void testGetNonExistentEntry() throws Exception {
    CRLCacheEntry result = cache.get("http://nonexistent.com/crl");

    assertNull(result);
  }

  @Test
  void testOverwriteExistingEntry() throws Exception {
    Instant firstTime = downloadTime.minus(1, ChronoUnit.HOURS);
    Instant secondTime = downloadTime;

    cache.put(TEST_CRL_URL, new CRLCacheEntry(testCRL, firstTime));
    assertEquals(1, countFilesInCache());

    cache.put(TEST_CRL_URL, new CRLCacheEntry(testCRL2, secondTime));
    assertEquals(1, countFilesInCache()); // Should still be one file

    CRLCacheEntry retrieved = cache.get(TEST_CRL_URL);
    assertNotNull(retrieved);
    assertEquals(testCRL2.getEncoded().length, retrieved.getCrl().getEncoded().length);
    assertTrue(
        Math.abs(secondTime.toEpochMilli() - retrieved.getDownloadTime().toEpochMilli()) < 1000);
  }

  @Test
  void testMultipleEntries() throws Exception {
    Instant downloadTime1 = downloadTime.minus(30, ChronoUnit.MINUTES);
    Instant downloadTime2 = downloadTime.minus(15, ChronoUnit.MINUTES);

    cache.put(TEST_CRL_URL, new CRLCacheEntry(testCRL, downloadTime1));
    cache.put(TEST_CRL_URL_2, new CRLCacheEntry(testCRL2, downloadTime2));

    assertEquals(2, countFilesInCache());

    CRLCacheEntry retrieved1 = cache.get(TEST_CRL_URL);
    CRLCacheEntry retrieved2 = cache.get(TEST_CRL_URL_2);

    assertNotNull(retrieved1);
    assertNotNull(retrieved2);
    assertEquals(testCRL.getEncoded().length, retrieved1.getCrl().getEncoded().length);
    assertEquals(testCRL2.getEncoded().length, retrieved2.getCrl().getEncoded().length);
  }

  @Test
  @DontRunOnWindows
  void testFilePermissions() throws Exception {
    cache = new CRLFileCache(tempCacheDir, Duration.ofMinutes(10));

    cache.put(TEST_CRL_URL, cacheEntry);

    try (Stream<Path> files = Files.list(tempCacheDir)) {
      Path crlFile = files.filter(Files::isRegularFile).findFirst().orElse(null);
      assertNotNull(crlFile);
      String permissions = PosixFilePermissions.toString(Files.getPosixFilePermissions(crlFile));
      assertEquals("rw-------", permissions);
    }
  }

  @Test
  void testCleanupExpiredCrl() throws Exception {
    cache.put(TEST_CRL_URL, cacheEntry);
    cache.put(TEST_CRL_URL_2, new CRLCacheEntry(createExpiredCrl(), downloadTime));

    assertEquals(2, countFilesInCache());

    cache.cleanup();

    assertEquals(1, countFilesInCache());
    assertNotNull(cache.get(TEST_CRL_URL));
    assertNull(cache.get(TEST_CRL_URL_2));
  }

  @Test
  void testCleanupEvictedEntries() throws Exception {
    cache.put(TEST_CRL_URL, new CRLCacheEntry(testCRL, downloadTime.minus(20, ChronoUnit.MINUTES)));
    cache.put(
        TEST_CRL_URL_2, new CRLCacheEntry(testCRL2, downloadTime.minus(5, ChronoUnit.MINUTES)));

    assertEquals(2, countFilesInCache());

    cache.cleanup();

    assertEquals(1, countFilesInCache());
    assertNull(cache.get(TEST_CRL_URL));
    assertNotNull(cache.get(TEST_CRL_URL_2));
  }

  @Test
  void testCleanupCorruptedFiles() throws Exception {
    // Create a corrupted file
    Path corruptedFile = tempCacheDir.resolve("corrupted.crl");
    Files.write(corruptedFile, "This is not a valid CRL".getBytes());

    assertEquals(1, countFilesInCache());

    cache.cleanup();

    // Corrupted file should be removed
    assertEquals(0, countFilesInCache());
    assertFalse(Files.exists(corruptedFile));
  }

  private X509CRL createValidCrl() throws Exception {
    Date futureDate = Date.from(downloadTime.plus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(futureDate);
  }

  private X509CRL createExpiredCrl() throws Exception {
    Date pastDate = Date.from(downloadTime.minus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(pastDate);
  }

  private int countFilesInCache() throws IOException {
    try (Stream<Path> files = Files.list(tempCacheDir)) {
      return (int) files.filter(Files::isRegularFile).count();
    }
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
                    // Ignore cleanup errors
                  }
                });
      }
    }
  }
}
