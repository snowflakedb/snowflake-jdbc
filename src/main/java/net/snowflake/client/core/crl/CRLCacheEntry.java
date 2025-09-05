package net.snowflake.client.core.crl;

import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;

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

  boolean isCrlExpired(Instant time) {
    return crl.getNextUpdate() != null && crl.getNextUpdate().toInstant().isBefore(time);
  }

  boolean isEvicted(Instant time, Duration cacheValidityTime) {
    return downloadTime.plus(cacheValidityTime).isBefore(time);
  }
}
