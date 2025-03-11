package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.Properties;
import net.snowflake.client.core.SFSessionProperty;
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
    assertEquals("testaccount", props.get("account"));
    assertEquals("snowflake", props.get("authenticator"));
    assertEquals("all", props.get("tracing"));
    assertEquals("application_name", props.get(SFSessionProperty.APPLICATION.getPropertyKey()));
    assertEquals("snowflake", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    assertEquals(
        "true", props.get(SFSessionProperty.JDBC_ARROW_TREAT_DECIMAL_AS_INT.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey()));
    assertEquals(
        "/some/path/file.json", props.get(SFSessionProperty.CLIENT_CONFIG_FILE.getPropertyKey()));
    assertEquals(
        "false", props.get(SFSessionProperty.DISABLE_GCS_DEFAULT_CREDENTIALS.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.DISABLE_SAML_URL_CHECK.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.DISABLE_SOCKS_PROXY.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.ENABLE_PATTERN_SEARCH.getPropertyKey()));
    assertEquals("DB_NAME", props.get(SFSessionProperty.DATABASE.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.ENABLE_PUT_GET.getPropertyKey()));
    assertEquals("5", props.get(SFSessionProperty.MAX_HTTP_RETRIES.getPropertyKey()));
    assertEquals("10", props.get(SFSessionProperty.NETWORK_TIMEOUT.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.OCSP_FAIL_OPEN.getPropertyKey()));
    assertEquals("proxyHost.com", props.get(SFSessionProperty.PROXY_HOST.getPropertyKey()));
    assertEquals("8080", props.get(SFSessionProperty.PROXY_PORT.getPropertyKey()));
    assertEquals("http", props.get(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey()));
    assertEquals("proxyUser", props.get(SFSessionProperty.PROXY_USER.getPropertyKey()));
    assertEquals("proxyPassword", props.get(SFSessionProperty.PROXY_PASSWORD.getPropertyKey()));
    assertEquals("3", props.get(SFSessionProperty.PUT_GET_MAX_RETRIES.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.STRINGS_QUOTED.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.ENABLE_DIAGNOSTICS.getPropertyKey()));
    assertEquals(
        "/some/path/allowlist.json",
        props.get(SFSessionProperty.DIAGNOSTICS_ALLOWLIST_FILE.getPropertyKey()));

    ds.setOauthToken("a_token");
    assertEquals("OAUTH", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    assertEquals("a_token", props.get(SFSessionProperty.TOKEN.getPropertyKey()));

    ds.setPasscodeInPassword(true);
    assertEquals("true", props.get(SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey()));
    assertEquals(
        "USERNAME_PASSWORD_MFA", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));

    ds.setPrivateKeyFile("key.p8", "pwd");
    assertEquals("key.p8", props.get(SFSessionProperty.PRIVATE_KEY_FILE.getPropertyKey()));
    assertEquals("pwd", props.get(SFSessionProperty.PRIVATE_KEY_PWD.getPropertyKey()));
    assertEquals("SNOWFLAKE_JWT", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));

    ds.setPasscodeInPassword(false);
    ds.setPasscode("a_passcode");
    assertEquals("false", props.get(SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey()));
    assertEquals(
        "USERNAME_PASSWORD_MFA", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    assertEquals("a_passcode", props.get(SFSessionProperty.PASSCODE.getPropertyKey()));

    ds.setPrivateKeyBase64("fake_key", "pwd");
    assertEquals("fake_key", props.get(SFSessionProperty.PRIVATE_KEY_BASE64.getPropertyKey()));
    assertEquals("pwd", props.get(SFSessionProperty.PRIVATE_KEY_PWD.getPropertyKey()));
    assertEquals("SNOWFLAKE_JWT", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
  }

  @Test
  public void testDataSourceWithoutUsernameOrPasswordThrowsExplicitException() {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();

    ds.setAccount("testaccount");
    ds.setAuthenticator("snowflake");
    Exception e = assertThrows(SnowflakeSQLException.class, ds::getConnection);
    assertEquals(
        "Cannot create connection because username is missing in DataSource properties.",
        e.getMessage());

    ds.setUser("testuser");
    e = assertThrows(SnowflakeSQLException.class, ds::getConnection);
    assertEquals(
        "Cannot create connection because password is missing in DataSource properties.",
        e.getMessage());
  }
}
