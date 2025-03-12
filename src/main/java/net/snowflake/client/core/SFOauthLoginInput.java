package net.snowflake.client.core;

@SnowflakeJdbcInternalApi
public class SFOauthLoginInput {

  private final String clientId;
  private final String clientSecret;
  private final String redirectUri;
  private final String authorizationUrl;
  private final String tokenRequestUrl;
  private final String scope;

  public SFOauthLoginInput(
      String clientId,
      String clientSecret,
      String redirectUri,
      String authorizationUrl,
      String tokenRequestUrl,
      String scope) {
    this.redirectUri = redirectUri;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.authorizationUrl = authorizationUrl;
    this.tokenRequestUrl = tokenRequestUrl;
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

  public String getAuthorizationUrl() {
    return authorizationUrl;
  }

  public String getTokenRequestUrl() {
    return tokenRequestUrl;
  }

  public String getScope() {
    return scope;
  }
}
