package net.snowflake.client.core;

import static net.snowflake.client.core.SessionUtilExternalBrowser.AuthExternalBrowserHandlers;

import com.amazonaws.util.StringUtils;
import java.net.URI;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.auth.oauth.AccessTokenProvider;
import net.snowflake.client.core.auth.oauth.OAuthAuthorizationCodeAccessTokenProvider;
import net.snowflake.client.core.auth.oauth.StateProvider;
import net.snowflake.client.core.auth.oauth.TokenResponseDTO;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class OAuthAuthorizationCodeFlowLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/oauth/authorization_code";
  private static final String SUCCESSFUL_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow.json";
  private static final String SUCCESSFUL_DPOP_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_dpop_flow.json";
  private static final String DPOP_NONCE_ERROR_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/dpop_nonce_error_flow.json";
  private static final String BROWSER_TIMEOUT_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/browser_timeout_authorization_error.json";
  private static final String INVALID_SCOPE_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/invalid_scope_error.json";
  private static final String INVALID_STATE_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/invalid_state_error.json";
  private static final String TOKEN_REQUEST_ERROR_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/token_request_error.json";
  private static final String CUSTOM_URLS_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/external_idp_custom_urls.json";

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthAuthorizationCodeFlowLatestIT.class);

  private final AuthExternalBrowserHandlers wiremockProxyRequestBrowserHandler =
      new WiremockProxyRequestBrowserHandler();

  private final AccessTokenProvider provider =
      new OAuthAuthorizationCodeAccessTokenProvider(
          wiremockProxyRequestBrowserHandler, new MockStateProvider(), 30);

  public OAuthAuthorizationCodeFlowLatestIT() throws SFException {}

  @Test
  public void successfulFlowScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8009/snowflake/oauth-redirect", null, null);

    TokenResponseDTO tokenResponse = provider.getAccessToken(loginInput);
    String accessToken = tokenResponse.getAccessToken();

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void successfulFlowDPoPScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_DPOP_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStubWithDPoPEnabled(
            "http://localhost:8012/snowflake/oauth-redirect", null, null);

    TokenResponseDTO tokenResponse = provider.getAccessToken(loginInput);
    String accessToken = tokenResponse.getAccessToken();

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void successfulFlowDPoPScenarioWithNonce() throws SFException {
    importMappingFromResources(DPOP_NONCE_ERROR_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStubWithDPoPEnabled(
            "http://localhost:8013/snowflake/oauth-redirect", null, null);

    TokenResponseDTO tokenResponse = provider.getAccessToken(loginInput);
    String accessToken = tokenResponse.getAccessToken();

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

    TokenResponseDTO tokenResponse = provider.getAccessToken(loginInput);
    String accessToken = tokenResponse.getAccessToken();

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void browserTimeoutFlowScenario() throws SFException {
    importMappingFromResources(BROWSER_TIMEOUT_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8004/snowflake/oauth-redirect", null, null);

    AccessTokenProvider provider =
        new OAuthAuthorizationCodeAccessTokenProvider(
            wiremockProxyRequestBrowserHandler, new MockStateProvider(), 1);
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
    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during authorization: invalid_scope, One or more scopes are not configured for the authorization server resource."));
  }

  @Test
  public void invalidStateFlowScenario() {
    importMappingFromResources(INVALID_STATE_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8010/snowflake/oauth-redirect", null, null);
    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during OAuth Authorization Code authentication: Invalid authorization request redirection state: invalidstate, expected: abc123"));
  }

  @Test
  public void tokenRequestErrorFlowScenario() {
    importMappingFromResources(TOKEN_REQUEST_ERROR_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8003/snowflake/oauth-redirect", null, null);

    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains("JDBC driver encountered communication error. Message: HTTP status=400"));
  }

  private SFLoginInput createLoginInputStub(
      String redirectUri, String authorizationUrl, String tokenRequestUrl) {
    SFLoginInput loginInputStub = new SFLoginInput();
    loginInputStub.setServerUrl(String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort));
    loginInputStub.setOauthLoginInput(
        new SFOauthLoginInput(
            "123", "123", redirectUri, authorizationUrl, tokenRequestUrl, "session:role:ANALYST"));
    loginInputStub.setSocketTimeout(Duration.ofMinutes(5));
    loginInputStub.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));

    return loginInputStub;
  }

  private SFLoginInput createLoginInputStubWithDPoPEnabled(
      String redirectUri, String authorizationUrl, String tokenRequestUrl) {
    SFLoginInput loginInputStub =
        createLoginInputStub(redirectUri, authorizationUrl, tokenRequestUrl);
    loginInputStub.setDPoPEnabled(true);
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

  static class MockStateProvider implements StateProvider<String> {

    @Override
    public String getState() {
      return "abc123";
    }
  }
}
