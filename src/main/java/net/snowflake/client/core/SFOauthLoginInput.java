package net.snowflake.client.core;

@SnowflakeJdbcInternalApi
public class SFOauthLoginInput {

  private static final String LOCAL_APPLICATION_CLIENT_CREDENTIAL = "LOCAL_APPLICATION";

  private String clientId;
  private String clientSecret;
  private final String redirectUri;
  private final String authorizationUrl;
  private final String tokenRequestUrl;
  private final String scope;
  private final boolean enableSingleUseRefreshTokens;

  public SFOauthLoginInput(
      String clientId,
      String clientSecret,
      String redirectUri,
      String authorizationUrl,
      String tokenRequestUrl,
      String scope) {
    this(clientId, clientSecret, redirectUri, authorizationUrl, tokenRequestUrl, scope, false);
  }

  public SFOauthLoginInput(
      String clientId,
      String clientSecret,
      String redirectUri,
      String authorizationUrl,
      String tokenRequestUrl,
      String scope,
      boolean enableSingleUseRefreshTokens) {
    this.redirectUri = redirectUri;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.authorizationUrl = authorizationUrl;
    this.tokenRequestUrl = tokenRequestUrl;
    this.scope = scope;
    this.enableSingleUseRefreshTokens = enableSingleUseRefreshTokens;
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

  public boolean getEnableSingleUseRefreshTokens() {
    return enableSingleUseRefreshTokens;
  }

  public void setLocalApplicationClientCredential() {
    this.clientId = LOCAL_APPLICATION_CLIENT_CREDENTIAL;
    this.clientSecret = LOCAL_APPLICATION_CLIENT_CREDENTIAL;
  }
}
