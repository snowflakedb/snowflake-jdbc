/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.NETWORK_ERROR;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

import java.net.SocketTimeoutException;
import java.security.cert.CertificateExpiredException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.SFOCSPException;
import net.snowflake.client.core.SFTrustManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for connection with OCSP mode mainly negative cases by injecting errors.
 *
 * <p>Ensure running hang_webserver.py in the backend to simulate connection hang.
 *
 * <p>hang_webserver.py 12345
 */
@Category(TestCategoryConnection.class)
public class ConnectionWithOCSPModeIT extends BaseJDBCTest {
  private final String testUser = "fakeuser";
  private final String testPassword = "testpassword";
  private final String testRevokedCertConnectString = "jdbc:snowflake://revoked.badssl.com/";

  private static int nameCounter = 0;

  @Before
  public void setUp() {
    SFTrustManager.deleteCache();
  }

  @After
  public void tearDown() {
    SFTrustManager.cleanTestSystemParameters();
  }

  private static synchronized String genTestConnectString() {
    String ret = "jdbc:snowflake://fakeaccount" + nameCounter + ".snowflakecomputing.com";
    ++nameCounter;
    return ret;
  }

  private static Throwable getCause(Throwable ex) {
    Throwable ex0 = ex;
    while (ex0.getCause() != null) {
      ex0 = ex0.getCause();
    }
    return ex0;
  }

  private Properties OCSPFailOpenProperties() {
    Properties properties = new Properties();
    properties.put("user", testUser);
    properties.put("password", testPassword);
    properties.put("ocspFailOpen", Boolean.TRUE.toString());
    properties.put("loginTimeout", "10");
    properties.put("tracing", "ALL");
    return properties;
  }

  private Properties OCSPFailClosedProperties() {
    Properties properties = new Properties();
    properties.put("user", testUser);
    properties.put("password", testPassword);
    properties.put("ocspFailOpen", Boolean.FALSE.toString());
    properties.put("loginTimeout", "10");
    properties.put("tracing", "ALL");
    return properties;
  }

  private Properties OCSPInsecureProperties() {
    Properties properties = new Properties();
    properties.put("user", testUser);
    properties.put("password", testPassword);
    properties.put("insecureMode", Boolean.TRUE.toString());
    properties.put("loginTimeout", "10");
    return properties;
  }

  /** Test OCSP response validity expired case. It should be ignored in FAIL_OPEN mode. */
  @Test
  public void testValidityExpiredOCSPResponseFailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /**
   * Test OCSP response validity expired case. INVALID_OCSP_RESPONSE_VALIDITY should be raised in
   * FAIL_CLOSED mode.
   */
  @Test
  public void testValidityExpiredOCSPResponseFailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      Throwable cause = getCause(ex);
      assertThat(cause, instanceOf(SFOCSPException.class));
      assertThat(
          ((SFOCSPException) cause).getErrorCode(),
          equalTo(OCSPErrorCode.INVALID_OCSP_RESPONSE_VALIDITY));
    }
  }

  /** Test no OCSP responder URL is attached. It should be ignored in FAIL_OPEN mode. */
  @Test
  public void testNoOCSPResponderURLFailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_NO_OCSP_RESPONDER_URL, Boolean.TRUE.toString());
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /**
   * Test no OCSP responder URL is attached. NO_OCSP_URL_ATTACHED should be raised in FAIL_CLOSED
   * mode.
   */
  @Test
  public void testNoOCSPResponderURLFailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_NO_OCSP_RESPONDER_URL, Boolean.TRUE.toString());
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      Throwable cause = getCause(ex);
      assertThat(cause, instanceOf(SFOCSPException.class));
      assertThat(
          ((SFOCSPException) cause).getErrorCode(), equalTo(OCSPErrorCode.NO_OCSP_URL_ATTACHED));
    }
  }

  /** Test OCSP response validity expired case. It should be ignored in INSECURE mode. */
  @Test
  public void testValidityExpiredOCSPResponseInsecure() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPInsecureProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /** Test invalid attached signing certificate is invalid. Should be ignored in FAIL_OPEN mode. */
  @Test
  public void testCertAttachedInvalidFailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INVALID_SIGNING_CERT, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /**
   * Test invalid attached signing certificate is invalid. EXPIRED_OCSP_SIGNING_CERTIFICATE should
   * be raised.
   */
  @Test
  public void testCertAttachedInvalidFailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INVALID_SIGNING_CERT, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      Throwable cause = getCause(ex);
      assertThat(cause, instanceOf(SFOCSPException.class));
      assertThat(
          ((SFOCSPException) cause).getErrorCode(),
          equalTo(OCSPErrorCode.EXPIRED_OCSP_SIGNING_CERTIFICATE));
    }
  }

  /** Test UNKNOWN certificate. Should be ignored in FAIL_OPEN mode. */
  @Test
  public void testUnknownOCSPCertFailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_UNKNOWN_STATUS, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /** Test UNKNOWN certificate. CERTIFICATE_STATUS_UNKNOWN should be raised. */
  @Test
  public void testUnknownOCSPCertFailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_UNKNOWN_STATUS, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      Throwable cause = getCause(ex);
      assertThat(cause, instanceOf(SFOCSPException.class));
      assertThat(
          ((SFOCSPException) cause).getErrorCode(),
          equalTo(OCSPErrorCode.CERTIFICATE_STATUS_UNKNOWN));
    }
  }

  /**
   * Test REVOKED certificate. CERTIFICATE_STATUS_REVOKED should be raised even in FAIL_OPEN
   * mode. @Test public void testRevokedCertFailOpen() { try {
   * DriverManager.getConnection(testRevokedCertConnectString, OCSPFailOpenProperties());
   * fail("should fail"); } catch (SQLException ex) { assertThat(ex,
   * instanceOf(SnowflakeSQLException.class)); assertThat(ex.getErrorCode(),
   * equalTo(NETWORK_ERROR.getMessageCode())); Throwable cause = getCause(ex); assertThat((cause,
   * instanceOf(SFOCSPException.class)) || (cause, instanceOf(CertificateExpiredException.class)));
   * assertThat(((SFOCSPException) cause).getErrorCode(),
   * equalTo(OCSPErrorCode.CERTIFICATE_STATUS_REVOKED)); } }
   */

  /**
   * Test REVOKED certificate. CERTIFICATE_STATUS_REVOKED should be raised. @Test public void
   * testRevokedCertFailClosed() { try { DriverManager.getConnection(testRevokedCertConnectString,
   * OCSPFailClosedProperties()); fail("should fail"); } catch (SQLException ex) { assertThat(ex,
   * instanceOf(SnowflakeSQLException.class)); assertThat(ex.getErrorCode(),
   * equalTo(NETWORK_ERROR.getMessageCode())); Throwable cause = getCause(ex); assertThat(cause,
   * instanceOf(SFOCSPException.class)); assertThat(((SFOCSPException) cause).getErrorCode(),
   * equalTo(OCSPErrorCode.CERTIFICATE_STATUS_REVOKED)); } }
   */

  /** Test OCSP Cache server hang and timeout. Should fall back to OCSP responder. */
  @Test
  public void testOCSPCacheServerTimeoutFailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT, "1000");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL, "http://localhost:12345/hang");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /**
   * Test OCSP Cache server hang and timeout. Should fall back to OCSP responder even in FAIL_CLOSED
   * mode.
   */
  @Test
  public void testOCSPCacheServerTimeoutFailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT, "1000");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL, "http://localhost:12345/hang");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.TRUE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertNull(ex.getCause());
    }
  }

  /** Test OCSP Responder hang and timeout. Should be ignored in FAIL_OPEN mode. */
  @Test
  public void testOCSPResponderTimeoutFailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT, "1000");
    System.setProperty(SFTrustManager.SF_OCSP_TEST_RESPONDER_URL, "http://localhost:12345/hang");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /** Test OCSP Responder hang and timeout. SocketTimeoutException exception should be raised. */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testOCSPResponderTimeoutFailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT, "1000");
    System.setProperty(SFTrustManager.SF_OCSP_TEST_RESPONDER_URL, "http://localhost:12345/hang");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      Throwable cause = getCause(ex);
      assertThat(cause, instanceOf(SocketTimeoutException.class));
    }
  }

  /** Test OCSP Responder returning HTTP 403. Should be ignored in FAIL_OPEN mode. */
  @Test
  public void testOCSPResponder403FailOpen() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_RESPONDER_URL, "http://localhost:12345/403");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailOpenProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(ex.getMessage(), containsString("HTTP status=403"));
      assertNull(ex.getCause());
    }
  }

  /**
   * Test OCSP Responder returning HTTP 403. HTTP 403 error should be raised. NOTE: don't confuse
   * with the FAIL_OPEN test case that also returns HTTP 403. It is raised because the test endpoint
   * is invalid.
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testOCSPResponder403FailClosed() {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_RESPONDER_URL, "http://localhost:12345/403");
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());
    try {
      DriverManager.getConnection(genTestConnectString(), OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(ex.getErrorCode(), equalTo(NETWORK_ERROR.getMessageCode()));
      assertThat(getCause(ex).getMessage(), containsString("StatusCode: 403"));
    }
  }

  /** Test Certificate Expired. Will fail in both FAIL_OPEN and FAIL_CLOSED. */
  @Test
  @Ignore("Issuer of root CA expired")
  // https://support.sectigo.com/articles/Knowledge/Sectigo-AddTrust-External-CA-Root-Expiring-May-30-2020
  public void testExpiredCert() {
    try {
      DriverManager.getConnection(
          "jdbc:snowflake://expired.badssl.com/", OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));
      assertThat(getCause(ex), instanceOf(CertificateExpiredException.class));
    }
  }

  /** Test Wrong host. Will fail in both FAIL_OPEN and FAIL_CLOSED. */
  @Test
  public void testWrongHost() {
    try {
      DriverManager.getConnection(
          "jdbc:snowflake://wrong.host.badssl.com/", OCSPFailClosedProperties());
      fail("should fail");
    } catch (SQLException ex) {
      assertThat(ex, instanceOf(SnowflakeSQLException.class));

      // The certificates used by badssl.com expired around 05/17/2022,
      // https://github.com/chromium/badssl.com/issues/504. After the certificates had been updated,
      // the exception seems to be changed from SSLPeerUnverifiedException to SSLHandshakeException.
      assertThat(
          ex.getCause(),
          anyOf(
              instanceOf(SSLPeerUnverifiedException.class),
              instanceOf(SSLHandshakeException.class)));
    }
  }
}
