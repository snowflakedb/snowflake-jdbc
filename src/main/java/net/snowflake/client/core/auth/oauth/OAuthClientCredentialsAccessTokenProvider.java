/*
 * Copyright (c) 2024-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.auth.oauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class OAuthClientCredentialsAccessTokenProvider implements AccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthClientCredentialsAccessTokenProvider.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public TokenResponseDTO getAccessToken(SFLoginInput loginInput) throws SFException {
    try {
      logger.debug("Starting OAuth authorization code authentication flow...");
      TokenRequest tokenRequest = buildTokenRequest(loginInput);
      return requestForAccessToken(loginInput, tokenRequest);
    } catch (Exception e) {
      logger.error(
          "Error during OAuth client credentials code flow. Verify configuration passed to driver and IdP (URLs, grant types, scope, etc.)",
          e);
      throw new SFException(e, ErrorCode.OAUTH_CLIENT_CREDENTIALS_FLOW_ERROR, e.getMessage());
    }
  }

  private TokenResponseDTO requestForAccessToken(SFLoginInput loginInput, TokenRequest tokenRequest)
      throws Exception {
    URI requestUri = tokenRequest.getEndpointURI();
    logger.debug(
        "Requesting OAuth access token from: {}{}",
        requestUri.getAuthority(),
        requestUri.getPath());
    String tokenResponse =
        HttpUtil.executeGeneralRequest(
            OAuthUtil.convertToBaseAuthorizationRequest(tokenRequest.toHTTPRequest()),
            loginInput.getLoginTimeout(),
            loginInput.getAuthTimeout(),
            loginInput.getSocketTimeoutInMillis(),
            0,
            loginInput.getHttpClientSettingsKey());
    TokenResponseDTO tokenResponseDTO =
        objectMapper.readValue(tokenResponse, TokenResponseDTO.class);
    logger.debug(
        "Received OAuth access token from: {}", requestUri.getAuthority() + requestUri.getPath());
    return tokenResponseDTO;
  }

  private static TokenRequest buildTokenRequest(SFLoginInput loginInput) {
    URI tokenRequestUrl =
        OAuthUtil.getTokenRequestUrl(loginInput.getOauthLoginInput(), loginInput.getServerUrl());
    ClientAuthentication clientAuthentication =
        new ClientSecretBasic(
            new ClientID(loginInput.getOauthLoginInput().getClientId()),
            new Secret(loginInput.getOauthLoginInput().getClientSecret()));
    Scope scope =
        new Scope(OAuthUtil.getScope(loginInput.getOauthLoginInput(), loginInput.getRole()));
    return new TokenRequest(
        tokenRequestUrl, clientAuthentication, new ClientCredentialsGrant(), scope);
  }
}
