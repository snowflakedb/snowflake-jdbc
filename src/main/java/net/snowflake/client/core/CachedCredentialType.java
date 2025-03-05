package net.snowflake.client.core;

enum CachedCredentialType {
  ID_TOKEN("ID_TOKEN"),
  MFA_TOKEN("MFATOKEN"),
  OAUTH_ACCESS_TOKEN("OAUTH_ACCESS_TOKEN"),
  OAUTH_REFRESH_TOKEN("OAUTH_REFRESH_TOKEN");

  private final String value;

  CachedCredentialType(String value) {
    this.value = value;
  }

  String getValue() {
    return value;
  }
}
