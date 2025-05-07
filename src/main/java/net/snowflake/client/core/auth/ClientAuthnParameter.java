package net.snowflake.client.core.auth;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public enum ClientAuthnParameter {
  LOGIN_NAME,
  PASSWORD,
  RAW_SAML_RESPONSE,
  ACCOUNT_NAME,
  CLIENT_APP_ID,
  CLIENT_APP_VERSION,
  EXT_AUTHN_DUO_METHOD,
  PASSCODE,
  CLIENT_ENVIRONMENT,
  AUTHENTICATOR,
  BROWSER_MODE_REDIRECT_PORT,
  SESSION_PARAMETERS,
  PROOF_KEY,
  TOKEN,
  OAUTH_TYPE,
  PROVIDER
}
