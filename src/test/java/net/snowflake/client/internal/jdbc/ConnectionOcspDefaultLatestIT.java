package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.OCSPMode;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFTrustManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Default-off OCSP against a real account: vanilla is disabled; explicit flags enable. */
@Tag(TestTags.CONNECTION)
public class ConnectionOcspDefaultLatestIT extends BaseJDBCTest {

  @BeforeEach
  public void setUp() {
    SFTrustManager.deleteCache();
  }

  @AfterEach
  public void tearDown() {
    SFTrustManager.cleanTestSystemParameters();
  }

  @Test
  public void vanillaConnectionDisablesOcsp() throws SQLException {
    try (Connection con = getConnection();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      SFSession session = con.unwrap(SnowflakeConnectionImpl.class).getSfSession();
      assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, session.getOCSPMode());
      assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, session.getHttpClientKey().getOcspMode());
    }
  }

  @Test
  public void disableFalseDoesNotEnableOcsp() throws SQLException {
    Properties props = new Properties();
    props.put("disableOCSPChecks", "false");
    try (Connection con = getConnection(props)) {
      assertEquals(
          OCSPMode.DISABLE_OCSP_CHECKS,
          con.unwrap(SnowflakeConnectionImpl.class).getSfSession().getOCSPMode());
    }
  }

  @Test
  public void ocspFailOpenFalseEnablesFailClosed() throws SQLException {
    Properties props = new Properties();
    props.put("ocspFailOpen", "false");
    try (Connection con = getConnection(props)) {
      assertEquals(
          OCSPMode.FAIL_CLOSED,
          con.unwrap(SnowflakeConnectionImpl.class).getSfSession().getOCSPMode());
    }
  }

  @Test
  public void disableBeatsExplicitFailOpen() throws SQLException {
    Properties failClosed = new Properties();
    failClosed.put("disableOCSPChecks", "true");
    failClosed.put("ocspFailOpen", "false");
    try (Connection con = getConnection(failClosed)) {
      assertEquals(
          OCSPMode.DISABLE_OCSP_CHECKS,
          con.unwrap(SnowflakeConnectionImpl.class).getSfSession().getOCSPMode());
    }

    Properties failOpen = new Properties();
    failOpen.put("disableOCSPChecks", "true");
    failOpen.put("ocspFailOpen", "true");
    try (Connection con = getConnection(failOpen)) {
      assertEquals(
          OCSPMode.DISABLE_OCSP_CHECKS,
          con.unwrap(SnowflakeConnectionImpl.class).getSfSession().getOCSPMode());
    }
  }

  @Test
  public void injectedValidityErrorIsIgnoredWhenOcspDisabled() throws SQLException {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR, Boolean.TRUE.toString());
    try (Connection con = getConnection();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      assertEquals(
          OCSPMode.DISABLE_OCSP_CHECKS,
          con.unwrap(SnowflakeConnectionImpl.class).getSfSession().getOCSPMode());
    } finally {
      System.clearProperty(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR);
    }
  }
}
