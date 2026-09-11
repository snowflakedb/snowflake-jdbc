package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * TLS version configuration must be rejected while properties are being set, with a proper {@link
 * ErrorCode}, rather than surfacing later as a raw {@code IllegalArgumentException} from socket
 * factory construction.
 */
public class SFBaseSessionTlsVersionTest {

  @Test
  public void shouldDefaultWhenUnset() throws Throwable {
    SFSession session = new SFSession();

    assertEquals(TlsVersion.DEFAULT_MIN, session.getMinTlsVersion());
    assertEquals(TlsVersion.DEFAULT_MAX, session.getMaxTlsVersion());
  }

  @Test
  public void shouldResolveConfiguredVersions() throws Throwable {
    SFSession session = new SFSession();
    session.addSFSessionProperty("MIN_TLS_VERSION", "TLSv1.3");
    session.addSFSessionProperty("MAX_TLS_VERSION", "TLSv1.3");

    assertEquals(TlsVersion.TLS_1_3, session.getMinTlsVersion());
    assertEquals(TlsVersion.TLS_1_3, session.getMaxTlsVersion());
  }

  @ParameterizedTest
  @ValueSource(strings = {"TLSv1.1", "TLSv1", "nonsense", ""})
  public void shouldRejectUnknownVersionWithErrorCode(String value) throws Throwable {
    SFSession session = new SFSession();
    session.addSFSessionProperty("MIN_TLS_VERSION", value);

    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, session::getMinTlsVersion);
    assertEquals(ErrorCode.INVALID_TLS_VERSION.getMessageCode().intValue(), thrown.getErrorCode());
  }

  @Test
  public void shouldRejectInvertedRangeWithErrorCode() throws Throwable {
    SFSession session = new SFSession();
    session.addSFSessionProperty("MIN_TLS_VERSION", "TLSv1.3");
    session.addSFSessionProperty("MAX_TLS_VERSION", "TLSv1.2");

    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, session::getMinTlsVersion);
    assertEquals(ErrorCode.INVALID_TLS_VERSION.getMessageCode().intValue(), thrown.getErrorCode());
  }

  /**
   * Strict precedence: when the system property is set the connection properties are ignored, so an
   * invalid or unavailable value there must not fail the connection.
   */
  @Test
  public void shouldNotValidateConnectionPropertiesWhenJvmPropertyGoverns() throws Throwable {
    String original = System.getProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1.2");
    try {
      SFSession session = new SFSession();
      session.addSFSessionProperty("MIN_TLS_VERSION", "not-a-tls-version");
      session.addSFSessionProperty("MAX_TLS_VERSION", "also-not-one");

      assertEquals(TlsVersion.DEFAULT_MIN, session.getMinTlsVersion());
      assertEquals(TlsVersion.DEFAULT_MAX, session.getMaxTlsVersion());
    } finally {
      if (original == null) {
        System.clearProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
      } else {
        System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, original);
      }
    }
  }

  /**
   * A jdk.tls.client.protocols value naming nothing this driver supports must be a SQLException.
   */
  @Test
  public void shouldRejectUnusableJvmPropertyWithErrorCode() throws Throwable {
    String original = System.getProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1,TLSv1.1");
    try {
      SFSession session = new SFSession();

      SnowflakeSQLException thrown =
          assertThrows(SnowflakeSQLException.class, session::getMinTlsVersion);
      assertEquals(
          ErrorCode.INVALID_TLS_VERSION.getMessageCode().intValue(), thrown.getErrorCode());
    } finally {
      if (original == null) {
        System.clearProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
      } else {
        System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, original);
      }
    }
  }

  @Test
  public void shouldPropagateTlsVersionsOntoHttpClientKey() throws Throwable {
    SFSession session = new SFSession();
    session.addSFSessionProperty("MIN_TLS_VERSION", "TLSv1.3");
    session.addSFSessionProperty("MAX_TLS_VERSION", "TLSv1.3");

    HttpClientSettingsKey key = session.getHttpClientKey();

    assertEquals(TlsVersion.TLS_1_3, key.getMinTlsVersion());
    assertEquals(TlsVersion.TLS_1_3, key.getMaxTlsVersion());
  }
}
