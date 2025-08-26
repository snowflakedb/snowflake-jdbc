package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class CRLInMemoryCacheTest {
  private static final String TEST_CRL_URL = "http://snowflake.com/test.crl";
  private static final String TEST_CRL_URL_2 = "http://snowflake.com/test2.crl";
  private static CertificateGeneratorUtil certGen;

  private CRLInMemoryCache cache;
  private X509CRL testCRL;
  private X509CRL testCRL2;

  @BeforeAll
  static void setUpClass() {
    certGen = new CertificateGeneratorUtil();
  }

  @BeforeEach
  void setUp() throws Exception {
    testCRL = createValidCrl();
    testCRL2 = createValidCrl();
    cache = new CRLInMemoryCache(Duration.ofMinutes(10));
  }

  @Test
  void testBasicPutAndGet() {
    Instant downloadTime = Instant.now();
    CRLCacheEntry entry = new CRLCacheEntry(testCRL, downloadTime);

    cache.put(TEST_CRL_URL, entry);
    CRLCacheEntry retrieved = cache.get(TEST_CRL_URL);

    assertNotNull(retrieved);
    assertEquals(testCRL, retrieved.getCrl());
    assertEquals(downloadTime, retrieved.getDownloadTime());
  }

  @Test
  void testGetNonExistentEntry() {
    CRLCacheEntry result = cache.get("http://nonexistent.com/crl");

    assertNull(result);
  }

  @Test
  void testOverwriteExistingEntry() {
    Instant firstTime = Instant.now().minus(1, ChronoUnit.HOURS);
    Instant secondTime = Instant.now();

    CRLCacheEntry firstEntry = new CRLCacheEntry(testCRL, firstTime);
    CRLCacheEntry secondEntry = new CRLCacheEntry(testCRL2, secondTime);

    cache.put(TEST_CRL_URL, firstEntry);
    cache.put(TEST_CRL_URL, secondEntry);

    CRLCacheEntry retrieved = cache.get(TEST_CRL_URL);
    assertNotNull(retrieved);
    assertEquals(testCRL2, retrieved.getCrl());
    assertEquals(secondTime, retrieved.getDownloadTime());
  }

  @Test
  void testCleanupExpiredCrl() throws Exception {
    CRLCacheEntry expiredEntry = new CRLCacheEntry(createExpiredCrl(), Instant.now());
    CRLCacheEntry validEntry = new CRLCacheEntry(createValidCrl(), Instant.now());

    cache.put(TEST_CRL_URL, expiredEntry);
    cache.put(TEST_CRL_URL_2, validEntry);

    // Both entries should be present before cleanup
    assertNotNull(cache.get(TEST_CRL_URL));
    assertNotNull(cache.get(TEST_CRL_URL_2));

    cache.cleanup();

    // Expired CRL should be removed, valid one should remain
    assertNull(cache.get(TEST_CRL_URL));
    assertNotNull(cache.get(TEST_CRL_URL_2));
  }

  @Test
  void testCleanupEvictedEntries() {
    CRLCacheEntry oldEntry =
        new CRLCacheEntry(testCRL, Instant.now().minus(20, ChronoUnit.MINUTES));
    CRLCacheEntry recentEntry =
        new CRLCacheEntry(testCRL2, Instant.now().minus(5, ChronoUnit.MINUTES));

    cache.put(TEST_CRL_URL, oldEntry);
    cache.put(TEST_CRL_URL_2, recentEntry);

    // Both entries should be present before cleanup
    assertNotNull(cache.get(TEST_CRL_URL));
    assertNotNull(cache.get(TEST_CRL_URL_2));

    cache.cleanup();

    // Old entry should be evicted, recent one should remain
    assertNull(cache.get(TEST_CRL_URL));
    assertNotNull(cache.get(TEST_CRL_URL_2));
  }

  private X509CRL createValidCrl() throws Exception {
    Date futureDate = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(futureDate);
  }

  private X509CRL createExpiredCrl() throws Exception {
    Date pastDate = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
    return certGen.generateCRL(pastDate);
  }
}
