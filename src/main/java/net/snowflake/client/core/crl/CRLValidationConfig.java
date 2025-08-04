package net.snowflake.client.core.crl;

class CRLValidationConfig {

  enum CertRevocationCheckMode {
    DISABLED,
    ENABLED,
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

  CertRevocationCheckMode getCertRevocationCheckMode() {
    return certRevocationCheckMode;
  }

  boolean isAllowCertificatesWithoutCrlUrl() {
    return allowCertificatesWithoutCrlUrl;
  }

  int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  int getReadTimeoutMs() {
    return readTimeoutMs;
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private CertRevocationCheckMode certRevocationCheckMode = CertRevocationCheckMode.DISABLED;
    private boolean allowCertificatesWithoutCrlUrl = false;
    private int connectionTimeoutMs = 30000; // 30 seconds
    private int readTimeoutMs = 30000; // 30 seconds

    Builder certRevocationCheckMode(CertRevocationCheckMode mode) {
      this.certRevocationCheckMode = mode;
      return this;
    }

    Builder allowCertificatesWithoutCrlUrl(boolean allow) {
      this.allowCertificatesWithoutCrlUrl = allow;
      return this;
    }

    Builder connectionTimeoutMs(int timeoutMs) {
      this.connectionTimeoutMs = timeoutMs;
      return this;
    }

    Builder readTimeoutMs(int timeoutMs) {
      this.readTimeoutMs = timeoutMs;
      return this;
    }

    CRLValidationConfig build() {
      return new CRLValidationConfig(this);
    }
  }
}
