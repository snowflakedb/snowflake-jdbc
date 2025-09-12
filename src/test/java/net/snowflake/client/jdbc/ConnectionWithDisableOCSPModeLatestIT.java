package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFTrustManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for connection with DisableOCSPchecks and insecuremode settings. */
@Tag(TestTags.CONNECTION)
public class ConnectionWithDisableOCSPModeLatestIT extends BaseJDBCTest {
  private static final int DISABLE_OCSP_INSECURE_MODE_MISMATCH = 200064;

  @BeforeEach
  public void setUp() {
    SFTrustManager.deleteCache();
  }

  @AfterEach
  public void tearDown() {
    SFTrustManager.cleanTestSystemParameters();
  }

  private static Stream<Arguments> testParameters() {
    return Stream.of(Arguments.of(true, true), Arguments.of(true, null), Arguments.of(null, true));
  }

  @ParameterizedTest
  @MethodSource("testParameters")
  public void shouldConnectIfDisableOCSPChecksAndInsecureModeAreSet(
      Boolean disableOCSPChecks, Boolean insecureMode) throws SQLException {
    Properties properties = getProperties(disableOCSPChecks, insecureMode);
    connectAndVerifySimpleQuery(properties);
  }

  private static Stream<Arguments> testParametersMismatch() {
    return Stream.of(Arguments.of(true, false), Arguments.of(false, true));
  }

  @ParameterizedTest
  @MethodSource("testParametersMismatch")
  public void shouldThrowWhenThereIsMismatchInDisableOCSPChecksAndInsecureMode(
      Boolean disableOCSPChecks, Boolean insecureMode) {
    Properties properties = getProperties(disableOCSPChecks, insecureMode);
    SQLException e =
        assertThrows(
            SQLException.class,
            () ->
                DriverManager.getConnection(
                    String.format(
                        "jdbc:snowflake://%s:%s", properties.get("host"), properties.get("port")),
                    properties));
    assertEquals(DISABLE_OCSP_INSECURE_MODE_MISMATCH, e.getErrorCode());
  }

  private Properties getProperties(Boolean disableOCSPChecks, Boolean insecureMode) {
    Map<String, String> params = getConnectionParameters();
    Properties props = new Properties();
    props.put("host", params.get("host"));
    props.put("port", params.get("port"));
    props.put("account", params.get("account"));
    props.put("user", params.get("user"));
    props.put("role", params.get("role"));

    // Handle authentication - prioritize private key, fallback to password
    if (params.get("private_key_file") != null) {
      props.put("private_key_file", params.get("private_key_file"));
      props.put("authenticator", params.get("authenticator"));
      if (params.get("private_key_pwd") != null) {
        props.put("private_key_pwd", params.get("private_key_pwd"));
      }
    } else if (params.get("password") != null) {
      props.put("password", params.get("password"));
    }

    props.put("warehouse", params.get("warehouse"));
    props.put("db", params.get("database"));
    props.put("schema", params.get("schema"));
    props.put("ssl", params.get("ssl"));
    if (disableOCSPChecks != null) {
      props.put("disableOCSPChecks", disableOCSPChecks);
    }
    if (insecureMode != null) {
      props.put("insecureMode", insecureMode);
    }
    return props;
  }
}
