package net.snowflake.client.core.crl;

import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

class CRLCacheEntry {
  private final X509CRL crl;
  private final Instant downloadTime;

  CRLCacheEntry(X509CRL crl, Instant downloadTime) {
    if (crl == null) {
      throw new IllegalArgumentException("CRL cannot be null");
    }
    if (downloadTime == null) {
      throw new IllegalArgumentException("Download time cannot be null");
    }
    this.crl = crl;
    this.downloadTime = downloadTime;
  }

  X509CRL getCrl() {
    return crl;
  }

  Instant getDownloadTime() {
    return downloadTime;
  }

  /**
   * Checks if this cache entry is expired based on CRL validity period.
   *
   * @param now current time
   * @return true if the CRL's nextUpdate time has passed
   */
  boolean isCrlExpired(Instant now) {
    return crl.getNextUpdate() != null && crl.getNextUpdate().toInstant().isBefore(now);
  }

  /**
   * Checks if this cache entry should be evicted based on cache validity time.
   *
   * @param now current time
   * @param cacheValidityTime maximum time to keep in cache
   * @return true if the entry should be evicted
   */
  boolean shouldEvict(Instant now, Duration cacheValidityTime) {
    return downloadTime.plus(cacheValidityTime).isBefore(now);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CRLCacheEntry that = (CRLCacheEntry) o;
    return Objects.equals(crl, that.crl) && Objects.equals(downloadTime, that.downloadTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(crl, downloadTime);
  }
}
