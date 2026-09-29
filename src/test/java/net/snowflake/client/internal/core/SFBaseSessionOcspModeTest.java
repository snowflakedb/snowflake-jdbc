package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.jdbc.DefaultSFConnectionHandler;
import net.snowflake.client.internal.jdbc.SnowflakeConnectString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Decision table for default-off OCSP: enable only via a set {@code ocspFailOpen} (connection
 * property or {@code -Dnet.snowflake.jdbc.ocspFailOpen}). {@code disableOCSPChecks=false} and
 * {@code insecureMode=false} are not opt-ins. {@code disableOCSPChecks=true} and the deprecated
 * {@code insecureMode=true} always turn OCSP off, including when {@code ocspFailOpen} is set.
 */
public class SFBaseSessionOcspModeTest {

  private static Stream<Arguments> modeMatrix() {
    // disableOCSPChecks, ocspFailOpen, insecureMode, expected mode
    return Stream.of(
        Arguments.of(null, null, null, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(null, true, null, OCSPMode.FAIL_OPEN),
        Arguments.of(null, false, null, OCSPMode.FAIL_CLOSED),
        Arguments.of(false, null, null, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(false, true, null, OCSPMode.FAIL_OPEN),
        Arguments.of(false, false, null, OCSPMode.FAIL_CLOSED),
        Arguments.of(true, null, null, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(true, true, null, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(true, false, null, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(null, null, true, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(null, null, false, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(true, true, false, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(false, null, true, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(null, true, true, OCSPMode.DISABLE_OCSP_CHECKS),
        Arguments.of(null, false, true, OCSPMode.DISABLE_OCSP_CHECKS));
  }

  @ParameterizedTest
  @MethodSource("modeMatrix")
  public void resolverMatchesDecisionTable(
      Boolean disable, Boolean failOpen, Boolean insecure, OCSPMode expected) {
    OcspModeResolver.Result result =
        OcspModeResolver.resolve(disable, failOpen, insecure, emptyEnv());
    assertEquals(expected, result.mode);
  }

  @ParameterizedTest
  @MethodSource("modeMatrix")
  public void sessionGetOCSPModeMatchesDecisionTable(
      Boolean disable, Boolean failOpen, Boolean insecure, OCSPMode expected)
      throws SFException, SnowflakeSQLException {
    SFSession session = sessionWith(disable, failOpen, insecure);
    assertEquals(expected, session.getOCSPMode());
  }

  @Test
  public void insecureModeAlwaysWarnsAndNeverThrows() {
    OcspModeResolver.Result mismatchDisable =
        OcspModeResolver.resolve(true, null, false, emptyEnv());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, mismatchDisable.mode);
    assertTrue(containsInsecureWarning(mismatchDisable.warnings));

    OcspModeResolver.Result mismatchEnable =
        OcspModeResolver.resolve(false, null, true, emptyEnv());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, mismatchEnable.mode);
    assertTrue(containsInsecureWarning(mismatchEnable.warnings));
  }

  @Test
  public void insecureModeTrueBeatsFailOpenAndFailClosed() {
    OcspModeResolver.Result failOpen = OcspModeResolver.resolve(null, true, true, emptyEnv());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, failOpen.mode);
    assertTrue(failOpen.warnings.contains(OcspModeResolver.FAIL_OPEN_IGNORED_WHEN_INSECURE_SET));
    assertTrue(containsInsecureWarning(failOpen.warnings));

    OcspModeResolver.Result failClosed = OcspModeResolver.resolve(null, false, true, emptyEnv());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, failClosed.mode);
    assertTrue(failClosed.warnings.contains(OcspModeResolver.FAIL_OPEN_IGNORED_WHEN_INSECURE_SET));
  }

  @Test
  public void disableTrueBeatsFailOpenAndFailClosed() {
    OcspModeResolver.Result failOpen = OcspModeResolver.resolve(true, true, null, emptyEnv());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, failOpen.mode);
    assertTrue(failOpen.warnings.contains(OcspModeResolver.FAIL_OPEN_IGNORED_WHEN_DISABLE_SET));

    OcspModeResolver.Result failClosed = OcspModeResolver.resolve(true, false, null, emptyEnv());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, failClosed.mode);
    assertTrue(failClosed.warnings.contains(OcspModeResolver.FAIL_OPEN_IGNORED_WHEN_DISABLE_SET));
  }

  @Test
  public void unsetPlusCacheDirSyspropWarns() {
    MapEnv env = emptyEnv();
    env.props.put(SFTrustManager.CACHE_DIR_PROP, "/tmp/ocsp");
    OcspModeResolver.Result result = OcspModeResolver.resolve(null, null, null, env);
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, result.mode);
    assertTrue(joined(result.warnings).contains(SFTrustManager.CACHE_DIR_PROP));
  }

  @Test
  public void unsetPlusCacheAndServerKnobsWarnNamed() {
    MapEnv env = emptyEnv();
    env.env.put(SFTrustManager.CACHE_DIR_ENV, "/tmp/ocsp-env");
    env.props.put(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL, "http://example/ocsp");
    env.env.put(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, "false");
    env.env.put(SFTrustManager.SF_OCSP_ACTIVATE_NEW_ENDPOINT, "1");
    OcspModeResolver.Result result = OcspModeResolver.resolve(null, null, null, env);
    String text = joined(result.warnings);
    assertTrue(text.contains(SFTrustManager.CACHE_DIR_ENV));
    assertTrue(text.contains(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL));
    assertTrue(text.contains(SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED));
    assertTrue(text.contains(SFTrustManager.SF_OCSP_ACTIVATE_NEW_ENDPOINT));
  }

  @Test
  public void testInjectorDoesNotWarnWhenDisabled() {
    MapEnv env = emptyEnv();
    env.props.put(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR, "true");
    OcspModeResolver.Result result = OcspModeResolver.resolve(null, null, null, env);
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, result.mode);
    assertFalse(
        joined(result.warnings).contains(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR));
  }

  @Test
  public void disableFalseStillWarnsCacheDirFailOpenDoesNot() {
    MapEnv env = emptyEnv();
    env.props.put(SFTrustManager.CACHE_DIR_PROP, "/tmp/ocsp");
    OcspModeResolver.Result disableFalse = OcspModeResolver.resolve(false, null, null, env);
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, disableFalse.mode);
    assertTrue(joined(disableFalse.warnings).contains(SFTrustManager.CACHE_DIR_PROP));

    OcspModeResolver.Result implicit = OcspModeResolver.resolve(null, true, null, env);
    assertEquals(OCSPMode.FAIL_OPEN, implicit.mode);
    assertFalse(joined(implicit.warnings).contains(SFTrustManager.CACHE_DIR_PROP));

    OcspModeResolver.Result disableWins = OcspModeResolver.resolve(true, true, null, env);
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, disableWins.mode);
    assertTrue(joined(disableWins.warnings).contains(SFTrustManager.CACHE_DIR_PROP));
    assertTrue(disableWins.warnings.contains(OcspModeResolver.FAIL_OPEN_IGNORED_WHEN_DISABLE_SET));
  }

  @Test
  public void getOCSPModeIsIdempotentAndDoesNotReresolve()
      throws SFException, SnowflakeSQLException {
    SFSession session = sessionWith(true, true, true);
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, session.getOCSPMode());
    assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, session.getOCSPMode());
  }

  @Test
  public void jvmFailOpenAloneEnablesMatchingMode() throws SQLException {
    String previous = System.getProperty(SessionUtil.OCSP_FAIL_OPEN_JVM);
    System.setProperty(SessionUtil.OCSP_FAIL_OPEN_JVM, "true");
    try {
      SFSession failOpen = sessionFromHandler(new Properties());
      assertEquals(OCSPMode.FAIL_OPEN, failOpen.getOCSPMode());
      assertEquals(
          Boolean.TRUE,
          failOpen.getConnectionPropertiesMap().get(SFSessionProperty.OCSP_FAIL_OPEN));
    } finally {
      restoreJvmProperty(SessionUtil.OCSP_FAIL_OPEN_JVM, previous);
    }

    System.setProperty(SessionUtil.OCSP_FAIL_OPEN_JVM, "false");
    try {
      SFSession failClosed = sessionFromHandler(new Properties());
      assertEquals(OCSPMode.FAIL_CLOSED, failClosed.getOCSPMode());
      assertEquals(
          Boolean.FALSE,
          failClosed.getConnectionPropertiesMap().get(SFSessionProperty.OCSP_FAIL_OPEN));
    } finally {
      restoreJvmProperty(SessionUtil.OCSP_FAIL_OPEN_JVM, previous);
    }
  }

  @Test
  public void disableOcspBeatsJvmFailOpen() throws SQLException {
    String previous = System.getProperty(SessionUtil.OCSP_FAIL_OPEN_JVM);
    System.setProperty(SessionUtil.OCSP_FAIL_OPEN_JVM, "true");
    try {
      Properties info = new Properties();
      info.put("disableOCSPChecks", "true");
      SFSession session = sessionFromHandler(info);
      assertEquals(OCSPMode.DISABLE_OCSP_CHECKS, session.getOCSPMode());
      assertEquals(
          Boolean.TRUE, session.getConnectionPropertiesMap().get(SFSessionProperty.OCSP_FAIL_OPEN));
    } finally {
      restoreJvmProperty(SessionUtil.OCSP_FAIL_OPEN_JVM, previous);
    }
  }

  @Test
  public void explicitSessionFailOpenEnablesMatchingMode()
      throws SFException, SnowflakeSQLException {
    SFSession failOpen = sessionWith(null, true, null);
    assertEquals(OCSPMode.FAIL_OPEN, failOpen.getOCSPMode());
    SFSession failClosed = sessionWith(null, false, null);
    assertEquals(OCSPMode.FAIL_CLOSED, failClosed.getOCSPMode());
  }

  private static SFSession sessionFromHandler(Properties extra) throws SQLException {
    Properties info = new Properties();
    info.put("account", "s3testaccount");
    info.put("user", "test");
    info.put("password", "test");
    info.putAll(extra);
    SnowflakeConnectString connectString =
        SnowflakeConnectString.parse("jdbc:snowflake://testaccount.localhost:8080", info);
    DefaultSFConnectionHandler handler = new DefaultSFConnectionHandler(connectString, true);
    handler.initializeConnection("jdbc:snowflake://testaccount.localhost:8080", info);
    return (SFSession) handler.getSFSession();
  }

  private static void restoreJvmProperty(String key, String previous) {
    if (previous == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, previous);
    }
  }

  private static SFSession sessionWith(Boolean disable, Boolean failOpen, Boolean insecure)
      throws SFException {
    SFSession session = new SFSession();
    if (disable != null) {
      session.addSFSessionProperty("disableOCSPChecks", disable);
    }
    if (failOpen != null) {
      session.addSFSessionProperty("ocspFailOpen", failOpen);
    }
    if (insecure != null) {
      session.addSFSessionProperty("insecureMode", insecure);
    }
    return session;
  }

  private static boolean containsInsecureWarning(List<String> warnings) {
    return warnings.stream().anyMatch(w -> w.contains("insecureMode"));
  }

  private static String joined(List<String> warnings) {
    return String.join(" | ", warnings);
  }

  private static MapEnv emptyEnv() {
    return new MapEnv();
  }

  private static final class MapEnv implements OcspModeResolver.Env {
    final Map<String, String> props = new HashMap<>();
    final Map<String, String> env = new HashMap<>();

    @Override
    public String getProperty(String key) {
      return props.get(key);
    }

    @Override
    public String getEnv(String key) {
      return env.get(key);
    }
  }
}
