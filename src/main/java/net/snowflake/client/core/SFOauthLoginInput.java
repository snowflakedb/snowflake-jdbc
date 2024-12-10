/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

@SnowflakeJdbcInternalApi
public class SFOauthLoginInput {

  private final String clientId;
  private final String clientSecret;
  private final String redirectUri;
  private final String externalAuthorizationUrl;
  private final String externalTokenRequestUrl;
  private final String scope;

  public SFOauthLoginInput(
      String clientId,
      String clientSecret,
      String redirectUri,
      String externalAuthorizationUrl,
      String externalTokenRequestUrl,
      String scope) {
    this.redirectUri = redirectUri;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.externalAuthorizationUrl = externalAuthorizationUrl;
    this.externalTokenRequestUrl = externalTokenRequestUrl;
    this.scope = scope;
  }

  public String getRedirectUri() {
    return redirectUri;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getExternalAuthorizationUrl() {
    return externalAuthorizationUrl;
  }

  public String getExternalTokenRequestUrl() {
    return externalTokenRequestUrl;
  }

  public String getScope() {
    return scope;
  }
}
