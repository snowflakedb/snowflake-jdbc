package net.snowflake.client.core;

/**
 * Configuration parameters for CRL validation. Based on the CRL validation design specification.
 */
public class CRLValidationConfig {

  /** Certificate revocation check mode. */
  public enum CertRevocationCheckMode {
    /** Disables CRL checking (with TLS handshake still in place) */
    DISABLED,

    /**
     * Fails the connection if certificate is revoked or there is other revocation status check
     * issue
     */
    ENABLED,

    /**
     * Fails the request for revoked certificate only. In case of any other problems assumes
     * certificate is not revoked
     */
    ADVISORY
  }

  private final CertRevocationCheckMode certRevocationCheckMode;
  private final boolean allowCertificatesWithoutCrlUrl;
  private final boolean enableCRLDiskCaching;
  private final boolean enableCRLInMemoryCaching;
  private final String crlResponseCacheDir;
  private final long crlValidityTimeMs;
  private final int connectionTimeoutMs;
  private final int readTimeoutMs;

  private CRLValidationConfig(Builder builder) {
    this.certRevocationCheckMode = builder.certRevocationCheckMode;
    this.allowCertificatesWithoutCrlUrl = builder.allowCertificatesWithoutCrlUrl;
    this.enableCRLDiskCaching = builder.enableCRLDiskCaching;
    this.enableCRLInMemoryCaching = builder.enableCRLInMemoryCaching;
    this.crlResponseCacheDir = builder.crlResponseCacheDir;
    this.crlValidityTimeMs = builder.crlValidityTimeMs;
    this.connectionTimeoutMs = builder.connectionTimeoutMs;
    this.readTimeoutMs = builder.readTimeoutMs;
  }

  public CertRevocationCheckMode getCertRevocationCheckMode() {
    return certRevocationCheckMode;
  }

  public boolean isAllowCertificatesWithoutCrlUrl() {
    return allowCertificatesWithoutCrlUrl;
  }

  public boolean isEnableCRLDiskCaching() {
    return enableCRLDiskCaching;
  }

  public boolean isEnableCRLInMemoryCaching() {
    return enableCRLInMemoryCaching;
  }

  public String getCrlResponseCacheDir() {
    return crlResponseCacheDir;
  }

  public long getCrlValidityTimeMs() {
    return crlValidityTimeMs;
  }

  public int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  public int getReadTimeoutMs() {
    return readTimeoutMs;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CertRevocationCheckMode certRevocationCheckMode = CertRevocationCheckMode.DISABLED;
    private boolean allowCertificatesWithoutCrlUrl = false;
    private boolean enableCRLDiskCaching = true;
    private boolean enableCRLInMemoryCaching = true;
    private String crlResponseCacheDir = null; // Will use default
    private long crlValidityTimeMs = 10L * 24 * 60 * 60 * 1000; // 10 days
    private int connectionTimeoutMs = 30000; // 30 seconds
    private int readTimeoutMs = 30000; // 30 seconds

    public Builder certRevocationCheckMode(CertRevocationCheckMode mode) {
      this.certRevocationCheckMode = mode;
      return this;
    }

    public Builder allowCertificatesWithoutCrlUrl(boolean allow) {
      this.allowCertificatesWithoutCrlUrl = allow;
      return this;
    }

    public Builder enableCRLDiskCaching(boolean enable) {
      this.enableCRLDiskCaching = enable;
      return this;
    }

    public Builder enableCRLInMemoryCaching(boolean enable) {
      this.enableCRLInMemoryCaching = enable;
      return this;
    }

    public Builder crlResponseCacheDir(String dir) {
      this.crlResponseCacheDir = dir;
      return this;
    }

    public Builder crlValidityTimeMs(long timeMs) {
      this.crlValidityTimeMs = timeMs;
      return this;
    }

    public Builder connectionTimeoutMs(int timeoutMs) {
      this.connectionTimeoutMs = timeoutMs;
      return this;
    }

    public Builder readTimeoutMs(int timeoutMs) {
      this.readTimeoutMs = timeoutMs;
      return this;
    }

    public CRLValidationConfig build() {
      return new CRLValidationConfig(this);
    }
  }
}
