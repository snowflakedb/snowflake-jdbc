package net.snowflake.client.core.crl;

/** Configuration parameters for CRL validation. */
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
  private final int connectionTimeoutMs;
  private final int readTimeoutMs;

  private CRLValidationConfig(Builder builder) {
    this.certRevocationCheckMode = builder.certRevocationCheckMode;
    this.allowCertificatesWithoutCrlUrl = builder.allowCertificatesWithoutCrlUrl;
    this.connectionTimeoutMs = builder.connectionTimeoutMs;
    this.readTimeoutMs = builder.readTimeoutMs;
  }

  public CertRevocationCheckMode getCertRevocationCheckMode() {
    return certRevocationCheckMode;
  }

  public boolean isAllowCertificatesWithoutCrlUrl() {
    return allowCertificatesWithoutCrlUrl;
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
