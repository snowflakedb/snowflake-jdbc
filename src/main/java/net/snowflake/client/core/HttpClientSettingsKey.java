package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.Serializable;
import java.util.Objects;
import net.snowflake.client.core.crl.CertRevocationCheckMode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * This class defines all non-static parameters needed to create an HttpClient object. It is used as
 * the key for the static hashmap of reusable http clients.
 */
public class HttpClientSettingsKey implements Serializable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HttpClientSettingsKey.class);
  private static final int DEFAULT_OCSP_RESPONDER_CONNECTION_TIMEOUT = 10000;
  private static final String SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT =
      "SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT";

  private OCSPMode ocspMode;
  private CertRevocationCheckMode revocationCheckMode;
  private boolean allowCertificatesWithoutCrlUrl;
  private boolean useProxy;
  private String proxyHost = "";
  private int proxyPort = 0;
  private String nonProxyHosts = "";
  private String proxyUser = "";
  private String proxyPassword = "";
  private String proxyProtocol = "http";
  // Adds a suffix to the user agent header in the http requests made by the jdbc driver.
  // More details in SNOW-717606
  private String userAgentSuffix = "";

  private Boolean gzipDisabled = false;
  private Integer ocspTimeout = null;

  public HttpClientSettingsKey(
      OCSPMode mode,
      String host,
      int port,
      String nonProxyHosts,
      String user,
      String password,
      String scheme,
      String userAgentSuffix,
      Boolean gzipDisabled) {
    this.useProxy = true;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.ocspTimeout = getDefaultOcspTimeout();
    this.proxyHost = !isNullOrEmpty(host) ? host.trim() : "";
    this.proxyPort = port;
    this.nonProxyHosts = !isNullOrEmpty(nonProxyHosts) ? nonProxyHosts.trim() : "";
    this.proxyUser = !isNullOrEmpty(user) ? user.trim() : "";
    this.proxyPassword = !isNullOrEmpty(password) ? password.trim() : "";
    this.proxyProtocol = !isNullOrEmpty(scheme) ? scheme.trim() : "http";
    this.gzipDisabled = gzipDisabled;
    this.userAgentSuffix = !isNullOrEmpty(userAgentSuffix) ? userAgentSuffix.trim() : "";
  }

  public HttpClientSettingsKey(OCSPMode mode) {
    this.useProxy = false;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.ocspTimeout = getDefaultOcspTimeout();
  }

  HttpClientSettingsKey(OCSPMode mode, String userAgentSuffix, Boolean gzipDisabled) {
    this(mode);
    this.userAgentSuffix = !isNullOrEmpty(userAgentSuffix) ? userAgentSuffix.trim() : "";
    this.gzipDisabled = gzipDisabled;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof HttpClientSettingsKey) {
      HttpClientSettingsKey comparisonKey = (HttpClientSettingsKey) obj;
      if (comparisonKey.ocspMode.getValue() == this.ocspMode.getValue()) {
        if (comparisonKey.revocationCheckMode == this.revocationCheckMode
            && comparisonKey.allowCertificatesWithoutCrlUrl
                == this.allowCertificatesWithoutCrlUrl) {
          if (comparisonKey.gzipDisabled.equals(this.gzipDisabled)) {
            if (comparisonKey.userAgentSuffix.equalsIgnoreCase(this.userAgentSuffix)) {
              if (Objects.equals(comparisonKey.ocspTimeout, this.ocspTimeout)) {
                if (!comparisonKey.useProxy && !this.useProxy) {
                  return true;
                } else if (comparisonKey.proxyHost.equalsIgnoreCase(this.proxyHost)
                    && comparisonKey.proxyPort == this.proxyPort
                    && comparisonKey.proxyUser.equalsIgnoreCase(this.proxyUser)
                    && comparisonKey.proxyPassword.equalsIgnoreCase(this.proxyPassword)
                    && comparisonKey.proxyProtocol.equalsIgnoreCase(this.proxyProtocol)) {
                  // update nonProxyHost if changed
                  if (!this.nonProxyHosts.equalsIgnoreCase(comparisonKey.nonProxyHosts)) {
                    comparisonKey.nonProxyHosts = this.nonProxyHosts;
                  }
                  return true;
                }
              }
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.ocspMode.getValue()
        + (this.proxyHost
                + this.proxyPort
                + this.proxyUser
                + this.proxyPassword
                + this.proxyProtocol)
            .hashCode()
        + Objects.hash(
            this.revocationCheckMode, this.allowCertificatesWithoutCrlUrl, this.ocspTimeout);
  }

  public OCSPMode getOcspMode() {
    return this.ocspMode;
  }

  public boolean usesProxy() {
    return this.useProxy;
  }

  public String getProxyHost() {
    return this.proxyHost;
  }

  public int getProxyPort() {
    return this.proxyPort;
  }

  public String getProxyUser() {
    return this.proxyUser;
  }

  public String getUserAgentSuffix() {
    return this.userAgentSuffix;
  }

  /**
   * Be careful of using this! Should only be called when password is later masked.
   *
   * @return proxy password
   */
  @SnowflakeJdbcInternalApi
  public String getProxyPassword() {
    return this.proxyPassword;
  }

  public String getNonProxyHosts() {
    return this.nonProxyHosts;
  }

  /**
   * @deprecated Use {@link #getProxyHttpProtocol()}
   * @return ProxyProtocol
   */
  @Deprecated
  public com.amazonaws.Protocol getProxyProtocol() {
    return this.proxyProtocol.equalsIgnoreCase("https")
        ? com.amazonaws.Protocol.HTTPS
        : com.amazonaws.Protocol.HTTP;
  }

  public HttpProtocol getProxyHttpProtocol() {
    return this.proxyProtocol.equalsIgnoreCase("https") ? HttpProtocol.HTTPS : HttpProtocol.HTTP;
  }

  public Boolean getGzipDisabled() {
    return gzipDisabled;
  }

  public CertRevocationCheckMode getRevocationCheckMode() {
    return revocationCheckMode;
  }

  public boolean isAllowCertificatesWithoutCrlUrl() {
    return allowCertificatesWithoutCrlUrl;
  }

  public void setRevocationCheckMode(CertRevocationCheckMode revocationCheckMode) {
    this.revocationCheckMode = revocationCheckMode;
  }

  public void setAllowCertificatesWithoutCrlUrl(boolean allowCertificatesWithoutCrlUrl) {
    this.allowCertificatesWithoutCrlUrl = allowCertificatesWithoutCrlUrl;
  }

  public int getOcspTimeout() {
    return ocspTimeout;
  }

  private static int getDefaultOcspTimeout() {
    int timeout = DEFAULT_OCSP_RESPONDER_CONNECTION_TIMEOUT;
    String configuredTimeout = systemGetProperty(SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT);
    if (!isNullOrEmpty(configuredTimeout)) {
      try {
        timeout = Integer.parseInt(configuredTimeout);
      } catch (Exception ex) {
        // ignore invalid override and keep default
        logger.debug("Invalid override for OCSP timeout: {}", configuredTimeout);
      }
    }
    return timeout;
  }

  @Override
  public String toString() {
    return "HttpClientSettingsKey["
        + "ocspMode="
        + ocspMode
        + ", revocationCheckMode="
        + revocationCheckMode
        + ", allowCertificatesWithoutCrlUrl="
        + allowCertificatesWithoutCrlUrl
        + ", useProxy="
        + useProxy
        + ", proxyHost='"
        + proxyHost
        + '\''
        + ", proxyPort="
        + proxyPort
        + ", nonProxyHosts='"
        + nonProxyHosts
        + '\''
        + ", proxyUser='"
        + proxyUser
        + '\''
        + ", proxyPassword is "
        + (proxyPassword.isEmpty() ? "not set" : "set")
        + ", proxyProtocol='"
        + proxyProtocol
        + '\''
        + ", userAgentSuffix='"
        + userAgentSuffix
        + '\''
        + ", gzipDisabled="
        + gzipDisabled
        + ", ocspTimeout="
        + ocspTimeout
        + ']';
  }
}
