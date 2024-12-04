package net.snowflake.client.jdbc;

import static net.snowflake.client.core.SessionUtilExternalBrowser.AuthExternalBrowserHandlers;

import com.amazonaws.util.StringUtils;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
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

  private static final String SUCCESSFUL_FLOW_SCENARIO_MAPPINGS =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"Successful OAuth authorization code flow\",\n"
          + "            \"requiredScenarioState\": \"Started\",\n"
          + "            \"newScenarioState\": \"Authorized\",\n"
          + "            \"request\": {\n"
          + "                \"urlPathPattern\": \"/oauth/authorize.*\",\n"
          + "                \"method\": \"GET\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200\n"
          + "            },\n"
          + "            \"serveEventListeners\": [\n"
          + "                {\n"
          + "                    \"name\": \"webhook\",\n"
          + "                    \"parameters\": {\n"
          + "                        \"method\": \"GET\",\n"
          + "                        \"url\": \"http://localhost:8001/snowflake/oauth-redirect?code=123\"\n"
          + "                    }\n"
          + "                }\n"
          + "            ]\n"
          + "        },\n"
          + "        {\n"
          + "            \"scenarioName\": \"Successful OAuth authorization code flow\",\n"
          + "            \"requiredScenarioState\": \"Authorized\",\n"
          + "            \"newScenarioState\": \"Acquired access token\",\n"
          + "            \"request\": {\n"
          + "                \"urlPathPattern\": \"/oauth/token-request.*\",\n"
          + "                \"method\": \"POST\",\n"
          + "                \"headers\": {\n"
          + "                    \"Authorization\": {\n"
          + "                        \"contains\": \"Basic\"\n"
          + "                    },\n"
          + "                    \"Content-Type\": {\n"
          + "                        \"contains\": \"application/x-www-form-urlencoded; charset=UTF-8\"\n"
          + "                    }\n"
          + "                },\n"
          + "                \"bodyPatterns\": [{\n"
          + "                    \"contains\": \"grant_type=authorization_code&code=123&redirect_uri=http%3A%2F%2Flocalhost%3A8001%2Fsnowflake%2Foauth-redirect&code_verifier=\"\n"
          + "                }]\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200,\n"
          + "                \"body\": \"{ \\\"access_token\\\" : \\\"access-token-123\\\", \\\"refresh_token\\\" : \\\"123\\\", \\\"token_type\\\" : \\\"Bearer\\\", \\\"username\\\" : \\\"user\\\", \\\"scope\\\" : \\\"refresh_token session:role:ANALYST\\\", \\\"expires_in\\\" : 600, \\\"refresh_token_expires_in\\\" : 86399, \\\"idpInitiated\\\" : false }\"\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }\n"
          + "}";

  private static final String BROWSER_TIMEOUT_SCENARIO_MAPPING =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"Browser Authorization timeout\",\n"
          + "            \"request\": {\n"
          + "                \"urlPathPattern\": \"/oauth/authorize.*\",\n"
          + "                \"method\": \"GET\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200,\n"
          + "                \"fixedDelayMilliseconds\": 5000\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }\n"
          + "}";

  private static final String INVALID_SCOPE_SCENARIO_MAPPING =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"Invalid scope authorization error\",\n"
          + "            \"request\": {\n"
          + "                \"urlPathPattern\": \"/oauth/authorize.*\",\n"
          + "                \"method\": \"GET\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200\n"
          + "            },\n"
          + "            \"serveEventListeners\": [\n"
          + "                {\n"
          + "                    \"name\": \"webhook\",\n"
          + "                    \"parameters\": {\n"
          + "                        \"method\": \"GET\",\n"
          + "                        \"url\": \"http://localhost:8002/snowflake/oauth-redirect?error=invalid_scope&error_description=One+or+more+scopes+are+not+configured+for+the+authorization+server+resource.\"\n"
          + "                    }\n"
          + "                }\n"
          + "            ]\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }\n"
          + "}";

  private static final String TOKEN_REQUEST_ERROR_SCENARIO_MAPPING =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"OAuth token request error\",\n"
          + "            \"requiredScenarioState\": \"Started\",\n"
          + "            \"newScenarioState\": \"Authorized\",\n"
          + "            \"request\": {\n"
          + "                \"urlPathPattern\": \"/oauth/authorize.*\",\n"
          + "                \"method\": \"GET\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200\n"
          + "            },\n"
          + "            \"serveEventListeners\": [\n"
          + "                {\n"
          + "                    \"name\": \"webhook\",\n"
          + "                    \"parameters\": {\n"
          + "                        \"method\": \"GET\",\n"
          + "                        \"url\": \"http://localhost:8003/snowflake/oauth-redirect?code=123\"\n"
          + "                    }\n"
          + "                }\n"
          + "            ]\n"
          + "        },\n"
          + "        {\n"
          + "            \"scenarioName\": \"OAuth token request error\",\n"
          + "            \"requiredScenarioState\": \"Authorized\",\n"
          + "            \"newScenarioState\": \"Token request error\",\n"
          + "            \"request\": {\n"
          + "                \"urlPathPattern\": \"/oauth/token-request.*\",\n"
          + "                \"method\": \"POST\",\n"
          + "                \"headers\": {\n"
          + "                    \"Authorization\": {\n"
          + "                        \"contains\": \"Basic\"\n"
          + "                    },\n"
          + "                    \"Content-Type\": {\n"
          + "                        \"contains\": \"application/x-www-form-urlencoded; charset=UTF-8\"\n"
          + "                    }\n"
          + "                },\n"
          + "                \"bodyPatterns\": [{\n"
          + "                    \"contains\": \"grant_type=authorization_code&code=123&redirect_uri=http%3A%2F%2Flocalhost%3A8003%2Fsnowflake%2Foauth-redirect&code_verifier=\"\n"
          + "                }]\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 400\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }\n"
          + "}";

  private static final Logger logger =
      LoggerFactory.getLogger(OauthAuthorizationCodeFlowLatestIT.class);

  AuthExternalBrowserHandlers wiremockProxyRequestBrowserHandler =
      new WiremockProxyRequestBrowserHandler();

  @Test
  public void successfulFlowScenario() throws SFException {
    importMapping(SUCCESSFUL_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8001/snowflake/oauth-redirect");

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    String accessToken = provider.getAccessToken(loginInput);

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void browserTimeoutFlowScenario() {
    importMapping(BROWSER_TIMEOUT_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8004/snowflake/oauth-redirect");

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 1);
    RuntimeException e =
        Assertions.assertThrows(RuntimeException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Authorization request timed out. Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration."));
  }

  @Test
  public void invalidScopeFlowScenario() {
    importMapping(INVALID_SCOPE_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8002/snowflake/oauth-redirect");

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    RuntimeException e =
        Assertions.assertThrows(RuntimeException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during authorization: invalid_scope, One or more scopes are not configured for the authorization server resource."));
  }

  @Test
  public void tokenRequestErrorFlowScenario() {
    importMapping(TOKEN_REQUEST_ERROR_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8003/snowflake/oauth-redirect");

    OauthAccessTokenProvider provider =
        new AuthorizationCodeFlowAccessTokenProvider(wiremockProxyRequestBrowserHandler, 30);
    RuntimeException e =
        Assertions.assertThrows(RuntimeException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "net.snowflake.client.jdbc.SnowflakeSQLException: JDBC driver encountered communication error. Message: HTTP status=400."));
  }

  private SFLoginInput createLoginInputStub(String redirectUri) {
    SFLoginInput loginInputStub = new SFLoginInput();
    loginInputStub.setServerUrl(String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort));
    loginInputStub.setClientSecret("123");
    loginInputStub.setClientId("123");
    loginInputStub.setRole("ANALYST");
    loginInputStub.setRedirectUri(redirectUri);
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
        HttpResponse response = client.execute(new HttpGet(ssoUrl));
        logger.debug(response.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void output(String msg) {
      // do nothing
    }
  }
}
