package net.snowflake.client.core;

enum CachedCredentialType {
  ID_TOKEN("ID_TOKEN"),
  MFA_TOKEN("MFATOKEN"),
  OAUTH_ACCESS_TOKEN("OAUTH_ACCESS_TOKEN"),
  OAUTH_REFRESH_TOKEN("OAUTH_REFRESH_TOKEN"),
  DPOP_BUNDLED_ACCESS_TOKEN(
      "DPOP_BUNDLED_ACCESS_TOKEN"); // contains '.' separated, base64 encoded access token and DPoP
  // public key

  private final String value;

  CachedCredentialType(String value) {
    this.value = value;
  }

  String getValue() {
    return value;
  }
}
