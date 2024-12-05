package net.snowflake.client.jdbc;

import static net.snowflake.client.core.SessionUtilExternalBrowser.AuthExternalBrowserHandlers;

import com.amazonaws.util.StringUtils;
import java.net.URI;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.auth.oauth.AuthorizationCodeFlowAccessTokenProvider;
import net.snowflake.client.core.auth.oauth.OauthAccessTokenProvider;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(TestTags.CORE)
public class OauthAuthorizationCodeFlowLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = "/oauth/authorization_code";
  private static final String SUCCESSFUL_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_scenario_mapping.json";
  private static final String BROWSER_TIMEOUT_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/browser_timeout_scenario_mapping.json";
  private static final String INVALID_SCOPE_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/invalid_scope_scenario_mapping.json";
  private static final String TOKEN_REQUEST_ERROR_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/token_request_error_scenario_mapping.json";
  private static final String CUSTOM_URLS_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/custom_urls_scenario_mapping.json";

  private static final Logger logger =
      LoggerFactory.getLogger(OauthAuthorizationCodeFlowLatestIT.class);

  private final AuthExternalBrowserHandlers wiremockProxyRequestBrowserHandler =
      new WiremockProxyRequestBrowserHandler();

  @Test
  public void successfulFlowScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8009/snowflake/oauth-redirect", null, null);

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    String accessToken = provider.getAccessToken(loginInput);

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void customUrlsScenario() throws SFException {
    importMappingFromResources(CUSTOM_URLS_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStub(
            "http://localhost:8007/snowflake/oauth-redirect",
            String.format("http://%s:%d/authorization", WIREMOCK_HOST, wiremockHttpPort),
            String.format("http://%s:%d/tokenrequest", WIREMOCK_HOST, wiremockHttpPort));

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    String accessToken = provider.getAccessToken(loginInput);

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void browserTimeoutFlowScenario() {
    importMappingFromResources(BROWSER_TIMEOUT_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8004/snowflake/oauth-redirect", null, null);

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 1);
    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Authorization request timed out. Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration."));
  }

  @Test
  public void invalidScopeFlowScenario() {
    importMappingFromResources(INVALID_SCOPE_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8002/snowflake/oauth-redirect", null, null);
    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during authorization: invalid_scope, One or more scopes are not configured for the authorization server resource."));
  }

  @Test
  public void tokenRequestErrorFlowScenario() {
    importMappingFromResources(TOKEN_REQUEST_ERROR_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8003/snowflake/oauth-redirect", null, null);

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during OAuth authentication: JDBC driver encountered communication error. Message: HTTP status=400."));
  }

  private SFLoginInput createLoginInputStub(
      String redirectUri, String externalAuthorizationUrl, String externalTokenUrl) {
    SFLoginInput loginInputStub = new SFLoginInput();
    loginInputStub.setServerUrl(String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort));
    loginInputStub.setOauthLoginInput(
        new SFOauthLoginInput(
            "123", "123", redirectUri, externalAuthorizationUrl, externalTokenUrl, "ANALYST"));
    loginInputStub.setSocketTimeout(Duration.ofMinutes(5));
    loginInputStub.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));

    return loginInputStub;
  }

  static class WiremockProxyRequestBrowserHandler implements AuthExternalBrowserHandlers {
    @Override
    public HttpPost build(URI uri) {
      // do nothing
      return null;
    }

    @Override
    public void openBrowser(String ssoUrl) {
      try (CloseableHttpClient client = HttpClients.createDefault()) {
        logger.debug("executing browser request to redirect uri: {}", ssoUrl);
        HttpResponse response = client.execute(new HttpGet(ssoUrl));
        if (response.getStatusLine().getStatusCode() != 200) {
          throw new RuntimeException("Invalid response from " + ssoUrl);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void output(String msg) {
      // do nothing
    }
  }
}
