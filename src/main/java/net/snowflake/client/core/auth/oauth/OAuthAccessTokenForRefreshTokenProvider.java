/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.auth.oauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import java.net.URI;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class OAuthAccessTokenForRefreshTokenProvider implements AccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthClientCredentialsAccessTokenProvider.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public TokenResponseDTO getAccessToken(SFLoginInput loginInput) throws SFException {
    try {
      logger.debug("Obtaining new OAuth access token using refresh token...");
      TokenRequest tokenRequest = buildTokenRequest(loginInput);
      return requestForAccessToken(loginInput, tokenRequest);
    } catch (Exception e) {
      logger.error("Error during OAuth refresh token flow.", e);
      throw new SFException(e, ErrorCode.OAUTH_REFRESH_TOKEN_FLOW_ERROR, e.getMessage());
    }
  }

  private TokenResponseDTO requestForAccessToken(SFLoginInput loginInput, TokenRequest tokenRequest)
      throws Exception {
    URI requestUri = tokenRequest.getEndpointURI();
    logger.debug(
        "Requesting new OAuth access token from: {}{}",
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
        "Received new OAuth access token from: {}{}",
        requestUri.getAuthority(),
        requestUri.getPath());
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
    RefreshToken refreshToken = new RefreshToken(loginInput.getOauthRefreshToken());
    return new TokenRequest(
        tokenRequestUrl, clientAuthentication, new RefreshTokenGrant(refreshToken), scope);
  }
}
