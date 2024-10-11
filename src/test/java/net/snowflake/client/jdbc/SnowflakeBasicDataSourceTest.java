/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.SQLException;
import java.util.Properties;
import net.snowflake.client.core.SFSessionProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Data source unit test */
public class SnowflakeBasicDataSourceTest {
  /** snow-37186 */
  @Test
  public void testSetLoginTimeout() throws SQLException {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();

    ds.setLoginTimeout(10);
    assertThat(ds.getLoginTimeout(), is(10));
  }

  @Test
  public void testDataSourceSetters() {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();

    ds.setTracing("all");
    ds.setApplication("application_name");
    ds.setAccount("testaccount");
    ds.setAuthenticator("snowflake");
    ds.setArrowTreatDecimalAsInt(true);
    ds.setAllowUnderscoresInHost(true);
    ds.setClientConfigFile("/some/path/file.json");
    ds.setDisableGcsDefaultCredentials(false);
    ds.setDisableSamlURLCheck(false);
    ds.setDisableSocksProxy(false);
    ds.setEnablePatternSearch(true);
    ds.setDatabaseName("DB_NAME");
    ds.setEnablePutGet(false);
    ds.setMaxHttpRetries(5);
    ds.setNetworkTimeout(10);
    ds.setOcspFailOpen(false);
    ds.setProxyHost("proxyHost.com");
    ds.setProxyPort(8080);
    ds.setProxyProtocol("http");
    ds.setProxyUser("proxyUser");
    ds.setProxyPassword("proxyPassword");
    ds.setPutGetMaxRetries(3);
    ds.setStringsQuotedForColumnDef(true);
    ds.setEnableDiagnostics(true);
    ds.setDiagnosticsAllowlistFile("/some/path/allowlist.json");

    Properties props = ds.getProperties();
    Assertions.assertEquals("testaccount", props.get("account"));
    Assertions.assertEquals("snowflake", props.get("authenticator"));
    Assertions.assertEquals("all", props.get("tracing"));
    Assertions.assertEquals(
        "application_name", props.get(SFSessionProperty.APPLICATION.getPropertyKey()));
    Assertions.assertEquals(
        "snowflake", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    Assertions.assertEquals(
        "true", props.get(SFSessionProperty.JDBC_ARROW_TREAT_DECIMAL_AS_INT.getPropertyKey()));
    Assertions.assertEquals(
        "true", props.get(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey()));
    Assertions.assertEquals(
        "/some/path/file.json", props.get(SFSessionProperty.CLIENT_CONFIG_FILE.getPropertyKey()));
    Assertions.assertEquals(
        "false", props.get(SFSessionProperty.DISABLE_GCS_DEFAULT_CREDENTIALS.getPropertyKey()));
    Assertions.assertEquals(
        "false", props.get(SFSessionProperty.DISABLE_SAML_URL_CHECK.getPropertyKey()));
    Assertions.assertEquals(
        "false", props.get(SFSessionProperty.DISABLE_SOCKS_PROXY.getPropertyKey()));
    Assertions.assertEquals(
        "true", props.get(SFSessionProperty.ENABLE_PATTERN_SEARCH.getPropertyKey()));
    Assertions.assertEquals("DB_NAME", props.get(SFSessionProperty.DATABASE.getPropertyKey()));
    Assertions.assertEquals("false", props.get(SFSessionProperty.ENABLE_PUT_GET.getPropertyKey()));
    Assertions.assertEquals("5", props.get(SFSessionProperty.MAX_HTTP_RETRIES.getPropertyKey()));
    Assertions.assertEquals("10", props.get(SFSessionProperty.NETWORK_TIMEOUT.getPropertyKey()));
    Assertions.assertEquals("false", props.get(SFSessionProperty.OCSP_FAIL_OPEN.getPropertyKey()));
    Assertions.assertEquals(
        "proxyHost.com", props.get(SFSessionProperty.PROXY_HOST.getPropertyKey()));
    Assertions.assertEquals("8080", props.get(SFSessionProperty.PROXY_PORT.getPropertyKey()));
    Assertions.assertEquals("http", props.get(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey()));
    Assertions.assertEquals("proxyUser", props.get(SFSessionProperty.PROXY_USER.getPropertyKey()));
    Assertions.assertEquals(
        "proxyPassword", props.get(SFSessionProperty.PROXY_PASSWORD.getPropertyKey()));
    Assertions.assertEquals("3", props.get(SFSessionProperty.PUT_GET_MAX_RETRIES.getPropertyKey()));
    Assertions.assertEquals("true", props.get(SFSessionProperty.STRINGS_QUOTED.getPropertyKey()));
    Assertions.assertEquals(
        "true", props.get(SFSessionProperty.ENABLE_DIAGNOSTICS.getPropertyKey()));
    Assertions.assertEquals(
        "/some/path/allowlist.json",
        props.get(SFSessionProperty.DIAGNOSTICS_ALLOWLIST_FILE.getPropertyKey()));

    ds.setOauthToken("a_token");
    Assertions.assertEquals("OAUTH", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    Assertions.assertEquals("a_token", props.get(SFSessionProperty.TOKEN.getPropertyKey()));

    ds.setPasscodeInPassword(true);
    Assertions.assertEquals(
        "true", props.get(SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey()));
    Assertions.assertEquals(
        "USERNAME_PASSWORD_MFA", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));

    ds.setPrivateKeyFile("key.p8", "pwd");
    Assertions.assertEquals(
        "key.p8", props.get(SFSessionProperty.PRIVATE_KEY_FILE.getPropertyKey()));
    Assertions.assertEquals("pwd", props.get(SFSessionProperty.PRIVATE_KEY_PWD.getPropertyKey()));
    Assertions.assertEquals(
        "SNOWFLAKE_JWT", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));

    ds.setPasscodeInPassword(false);
    ds.setPasscode("a_passcode");
    Assertions.assertEquals(
        "false", props.get(SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey()));
    Assertions.assertEquals(
        "USERNAME_PASSWORD_MFA", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    Assertions.assertEquals("a_passcode", props.get(SFSessionProperty.PASSCODE.getPropertyKey()));

    ds.setPrivateKeyBase64("fake_key", "pwd");
    Assertions.assertEquals(
        "fake_key", props.get(SFSessionProperty.PRIVATE_KEY_BASE64.getPropertyKey()));
    Assertions.assertEquals("pwd", props.get(SFSessionProperty.PRIVATE_KEY_PWD.getPropertyKey()));
    Assertions.assertEquals(
        "SNOWFLAKE_JWT", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
  }
}
