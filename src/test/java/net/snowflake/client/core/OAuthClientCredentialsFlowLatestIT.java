/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.amazonaws.util.StringUtils;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.auth.oauth.AccessTokenProvider;
import net.snowflake.client.core.auth.oauth.OAuthClientCredentialsAccessTokenProvider;
import net.snowflake.client.core.auth.oauth.TokenResponseDTO;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class OAuthClientCredentialsFlowLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/oauth/client_credentials";
  private static final String SUCCESSFUL_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow.json";
  private static final String TOKEN_REQUEST_ERROR_SCENARIO_MAPPING =
      SCENARIOS_BASE_DIR + "/token_request_error.json";

  @Test
  public void successfulFlowScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput =
        createLoginInputStub("http://localhost:8009/snowflake/oauth-redirect");

    AccessTokenProvider provider = new OAuthClientCredentialsAccessTokenProvider();
    TokenResponseDTO tokenResponse = provider.getAccessToken(loginInput);
    String accessToken = tokenResponse.getAccessToken();

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
}