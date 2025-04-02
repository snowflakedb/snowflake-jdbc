package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFTrustManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Tests for connection with DisableOCSPchecks and insecuremode settings. */
@Tag(TestTags.CONNECTION)
public class ConnectionWithDisableOCSPModeLatestIT extends BaseJDBCTest {
  public static final int INVALID_CONNECTION_INFO_CODE = 390100;
  private static final int DISABLE_OCSP_INSECURE_MODE_MISMATCH = 200064;
  public static final int BAD_REQUEST_GS_CODE = 390400;

  @BeforeEach
  public void setUp() {
    SFTrustManager.deleteCache();
  }

  @AfterEach
  public void tearDown() {
    SFTrustManager.cleanTestSystemParameters();
  }

  /**
   * Test connectivity with disableOCSPChecksMode and insecure mode enabled. This test applies to
   * driver versions after 3.21.0
   */
  @Disabled("Disable due to changed error response in backend. Follow up: SNOW-2021007")
  @Test
  public void testDisableOCSPChecksModeAndInsecureModeSet() throws SQLException {

    boolean disableOCSPChecks = true;
    boolean insecureMode = true;
    assertThat(
        returnErrorCodeFromConnection(disableOCSPChecks, insecureMode),
        anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
  }

  /**
   * Test production connectivity with only disableOCSPChecksMode enabled. This test applies to
   * driver versions after 3.21.0
   */
  @Disabled("Disable due to changed error response in backend. Follow up: SNOW-2021007")
  @Test
  public void testDisableOCSPChecksModeSet() throws SQLException {
    boolean disableOCSPChecks = true;
    assertThat(
        returnErrorCodeFromConnection(disableOCSPChecks, null),
        anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
  }

  /**
   * Test production connectivity with only insecureMode enabled. This test applies to driver
   * versions after 3.21.0
   */
  @Disabled("Disable due to changed error response in backend. Follow up: SNOW-2021007")
  @Test
  public void testInsecureModeSet() throws SQLException {
    boolean insecureMode = true;
    assertThat(
        returnErrorCodeFromConnection(null, insecureMode),
        anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
  }

  /**
   * Test production connectivity with disableOCSPChecksMode enabled AND insecureMode disabled. This
   * test applies to driver versions after 3.21.0
   */
  @Disabled("Disable due to changed error response in backend. Follow up: SNOW-2021007")
  @Test
  public void testDisableOCSPChecksModeAndInsecureModeMismatched() throws SQLException {
    boolean disableOCSPChecks = true;
    boolean insecureMode = false;
    assertThat(
        returnErrorCodeFromConnection(disableOCSPChecks, insecureMode),
        anyOf(is(DISABLE_OCSP_INSECURE_MODE_MISMATCH)));
  }

  /**
   * Helper method to return error code from connection.
   *
   * @param disableOSCPChecks
   * @param isInsecureMode
   * @return SF Error code
   * @throws SQLException
   */
  public int returnErrorCodeFromConnection(Boolean disableOSCPChecks, Boolean isInsecureMode)
      throws SQLException {
    String deploymentUrl = "jdbc:snowflake://sfcsupport.snowflakecomputing.com";
    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    if (disableOSCPChecks != null) {
      properties.put("disableOCSPChecks", disableOSCPChecks);
    }
    if (isInsecureMode != null) {
      properties.put("insecureMode", isInsecureMode);
    }

    SQLException thrown =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(deploymentUrl, properties);
            });

    return (thrown.getErrorCode());
  }
}
