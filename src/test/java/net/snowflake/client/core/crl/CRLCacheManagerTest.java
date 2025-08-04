package net.snowflake.client.core.crl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Security;
import java.security.cert.X509CRL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import net.snowflake.client.category.TestTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class CRLCacheManagerTest {
  private static final String TEST_CRL_URL = "http://snowflake.com/test.crl";

  private static CertificateGeneratorUtil certGen;

  private CRLInMemoryCache mockMemoryCache;
  private CRLFileCache mockFileCache;
  private CRLCacheManager cacheManager;
  private X509CRL testCrl;
  private Instant testDownloadTime;
  private CRLCacheEntry testCacheEntry;

  @BeforeAll
  static void setUpClass() {
    Security.addProvider(new BouncyCastleProvider());
    certGen = new CertificateGeneratorUtil();
  }

  @BeforeEach
  void setUp() {
    mockMemoryCache = mock(CRLInMemoryCache.class);
    mockFileCache = mock(CRLFileCache.class);

    testCrl = createTestCrl();
    testDownloadTime = Instant.now().minus(30, ChronoUnit.MINUTES);
    testCacheEntry = new CRLCacheEntry(testCrl, testDownloadTime);

    cacheManager = new CRLCacheManager(mockMemoryCache, mockFileCache);
  }

  @Test
  void shouldReturnCacheEntryWhenMemoryCacheHit() {
    when(mockMemoryCache.get(TEST_CRL_URL)).thenReturn(testCacheEntry);

    CRLCacheEntry result = cacheManager.get(TEST_CRL_URL);

    assertNotNull(result);
    assertEquals(testCrl, result.getCrl());
    assertEquals(testDownloadTime, result.getDownloadTime());
    verify(mockMemoryCache).get(TEST_CRL_URL);
    verify(mockFileCache, never()).get(TEST_CRL_URL);
  }

  @Test
  void shouldPromoteFileCacheHitToMemoryCache() {
    when(mockMemoryCache.get(TEST_CRL_URL)).thenReturn(null);
    when(mockFileCache.get(TEST_CRL_URL)).thenReturn(testCacheEntry);

    CRLCacheEntry result = cacheManager.get(TEST_CRL_URL);

    assertNotNull(result);
    assertEquals(testCrl, result.getCrl());
    assertEquals(testDownloadTime, result.getDownloadTime());
    verify(mockMemoryCache).get(TEST_CRL_URL);
    verify(mockFileCache).get(TEST_CRL_URL);
    verify(mockMemoryCache).put(TEST_CRL_URL, testCacheEntry);
  }

  @Test
  void shouldReturnNullWhenBothCachesMiss() {
    when(mockMemoryCache.get(TEST_CRL_URL)).thenReturn(null);
    when(mockFileCache.get(TEST_CRL_URL)).thenReturn(null);

    CRLCacheEntry result = cacheManager.get(TEST_CRL_URL);

    assertNull(result);
    verify(mockMemoryCache).get(TEST_CRL_URL);
    verify(mockFileCache).get(TEST_CRL_URL);
    verify(mockMemoryCache, never()).put(any(), any());
  }

  @Test
  void shouldPutToBothMemoryAndFileCache() {
    Instant putTime = Instant.now();

    cacheManager.put(TEST_CRL_URL, testCrl, putTime);

    verify(mockMemoryCache).put(eq(TEST_CRL_URL), any(CRLCacheEntry.class));
    verify(mockFileCache).put(TEST_CRL_URL, testCrl, putTime);
  }

  @Test
  void shouldNotPromoteToMemoryCacheWhenFileCacheReturnsNull() {
    when(mockMemoryCache.get(TEST_CRL_URL)).thenReturn(null);
    when(mockFileCache.get(TEST_CRL_URL)).thenReturn(null);

    CRLCacheEntry result = cacheManager.get(TEST_CRL_URL);

    assertNull(result);
    verify(mockMemoryCache).get(TEST_CRL_URL);
    verify(mockFileCache).get(TEST_CRL_URL);
    verify(mockMemoryCache, never()).put(any(), any());
  }

  @Test
  void shouldCreateDifferentCacheEntriesForSameCrlWithDifferentDownloadTimes() {
    Instant firstPutTime = Instant.now().minus(1, ChronoUnit.HOURS);
    Instant secondPutTime = Instant.now();

    cacheManager.put(TEST_CRL_URL, testCrl, firstPutTime);
    cacheManager.put(TEST_CRL_URL, testCrl, secondPutTime);

    verify(mockMemoryCache, times(2)).put(eq(TEST_CRL_URL), any(CRLCacheEntry.class));
    verify(mockFileCache).put(TEST_CRL_URL, testCrl, firstPutTime);
    verify(mockFileCache).put(TEST_CRL_URL, testCrl, secondPutTime);
  }

  private X509CRL createTestCrl() {
    try {
      Date futureDate = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
      byte[] crlBytes = certGen.generateCRL(futureDate);
      return certGen.convertBytesToCRL(crlBytes);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test CRL", e);
    }
  }
}
