/*
 * Copyright (c) 2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFTrustManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

  /** Test connectivity with disableOCSPChecksMode and insecure mode enabled. */
  @Test
  public void testDisableOCSPChecksModeAndInsecureMode() throws SQLException {

    String deploymentUrl =
        "jdbc:snowflake://sfcsupport.snowflakecomputing.com?disableOCSPChecks=true&insecureMode=true";
    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(deploymentUrl, properties);
            });

    assertThat(
        thrown.getErrorCode(), anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
  }

  /** Test connectivity with disableOCSPChecksMode enabled and insecure mode disabled. */
  @Test
  public void testDisableOCSPChecksModeAndInsecureModeMismatched() throws SQLException {

    String deploymentUrl =
        "jdbc:snowflake://sfcsupport.snowflakecomputing.com?disableOCSPChecks=true&insecureMode=false";
    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(deploymentUrl, properties);
            });

    assertThat(thrown.getErrorCode(), anyOf(is(DISABLE_OCSP_INSECURE_MODE_MISMATCH)));
  }

  /** Test production connectivity with only disableOCSPChecksMode enabled. */
  @Test
  public void testDisableOCSPChecksModeSet() throws SQLException {

    String deploymentUrl =
        "jdbc:snowflake://sfcsupport.snowflakecomputing.com?disableOCSPChecks=true";
    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(deploymentUrl, properties);
            });

    assertThat(
        thrown.getErrorCode(), anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
  }

  /** Test production connectivity with insecure mode enabled. */
  @Test
  public void testEnableInsecureMode() throws SQLException {
    String deploymentUrl = "jdbc:snowflake://sfcsupport.snowflakecomputing.com?insecureMode=true";
    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    try {
      DriverManager.getConnection(deploymentUrl, properties);
      fail();
    } catch (SQLException e) {
      assertThat(
          e.getErrorCode(), anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
    }
  }
}
