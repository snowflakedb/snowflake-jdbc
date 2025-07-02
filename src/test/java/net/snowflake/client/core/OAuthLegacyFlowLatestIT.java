package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OAuthLegacyFlowLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/oauth/legacy_oauth";
  private static final String EXPIRED_TOKEN_SCENARIO = SCENARIOS_BASE_DIR + "/token_expired.json";

  @Test
  public void shouldThrowExpirationExceptionUponExpiredTokenResponse() {
    SFLoginInput loginInput = createLoginInputStub();
    importMappingFromResources(EXPIRED_TOKEN_SCENARIO);
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SessionUtil.openSession(loginInput, new HashMap<>(), "INFO"));
    assertTrue(
        e.getMessage().contains("OAuth access token expired. [1172527951366]"),
        "Expected expiration error message, but got: " + e.getMessage());
  }

  private SFLoginInput createLoginInputStub() {
    SFLoginInput input = new SFLoginInput();
    input.setAuthenticator(AuthenticatorType.OAUTH.name());
    input.setOriginalAuthenticator(AuthenticatorType.OAUTH.name());
    input.setServerUrl(String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort));
    input.setUserName("MOCK_USERNAME");
    input.setAccountName("MOCK_ACCOUNT_NAME");
    input.setAppId("MOCK_APP_ID");
    input.setAppVersion("MOCK_APP_VERSION");
    input.setToken("expired-access-token-123");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setBrowserResponseTimeout(Duration.ofSeconds(5));
    input.setBrowserHandler(
        new OAuthAuthorizationCodeFlowLatestIT.WiremockProxyRequestBrowserHandler());
    input.setLoginTimeout(1000);
    HashMap<String, Object> sessionParameters = new HashMap<>();
    sessionParameters.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", "true");
    input.setSessionParameters(sessionParameters);
    input.setOauthLoginInput(new SFOauthLoginInput(null, null, null, null, null, null));
    return input;
  }
}
