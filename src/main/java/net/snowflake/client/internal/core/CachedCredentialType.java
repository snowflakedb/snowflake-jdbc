package net.snowflake.client.internal.core;

enum CachedCredentialType {
  ID_TOKEN("IdToken"),
  MFA_TOKEN("MfaToken"),
  OAUTH_ACCESS_TOKEN("OauthAccessToken"),
  OAUTH_REFRESH_TOKEN("OauthRefreshToken"),
  DPOP_BUNDLED_ACCESS_TOKEN(
      "DpopBundledAccessToken"); // contains '.' separated, base64 encoded access token and DPoP
  // public key

  private final String value;

  CachedCredentialType(String value) {
    this.value = value;
  }

  String getValue() {
    return value;
  }
}
