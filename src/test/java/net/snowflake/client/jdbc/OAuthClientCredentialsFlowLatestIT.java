/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

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
import net.snowflake.client.core.auth.oauth.AccessTokenProvider;
import net.snowflake.client.core.auth.oauth.OAuthClientCredentialsAccessTokenProvider;
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
public class OAuthClientCredentialsFlowLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = "/oauth/client_credentials";
  private static final String SUCCESSFUL_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_scenario_mapping.json";
  private static final String TOKEN_REQUEST_ERROR_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/token_request_error_scenario_mapping.json";

  private static final Logger logger =
      LoggerFactory.getLogger(OAuthClientCredentialsFlowLatestIT.class);

  @Test
  public void successfulFlowScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8009/snowflake/oauth-redirect");

    AccessTokenProvider provider = new OAuthClientCredentialsAccessTokenProvider();
    String accessToken = provider.getAccessToken(loginInput);

    Assertions.assertFalse(StringUtils.isNullOrEmpty(accessToken));
    Assertions.assertEquals("access-token-123", accessToken);
  }

  @Test
  public void tokenRequestErrorFlowScenario() {
    importMappingFromResources(TOKEN_REQUEST_ERROR_SCENARIO_MAPPING);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8003/snowflake/oauth-redirect");

    AccessTokenProvider provider = new OAuthClientCredentialsAccessTokenProvider();
    SFException e =
        Assertions.assertThrows(SFException.class, () -> provider.getAccessToken(loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains("JDBC driver encountered communication error. Message: HTTP status=400"));
  }

  private SFLoginInput createLoginInputStub(String redirectUri) {
    SFLoginInput loginInputStub = new SFLoginInput();
    loginInputStub.setServerUrl(String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort));
    loginInputStub.setOauthLoginInput(
        new SFOauthLoginInput(
            "123",
            "123",
            redirectUri,
            null,
            String.format("http://%s:%d/oauth/token-request", WIREMOCK_HOST, wiremockHttpPort),
            "session:role:ANALYST"));
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
