package net.snowflake.client.core.auth;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public enum AuthenticatorType {
  /*
   * regular login username+password via Snowflake, may or may not have MFA
   */
  SNOWFLAKE,

  /*
   * federated authentication, OKTA as IDP
   */
  OKTA,

  /*
   * Web browser based authenticator for SAML 2.0 compliant
   * service/application
   */
  EXTERNALBROWSER,

  /*
   * OAUTH 2.0 flow
   */
  OAUTH,

  /*
   * Snowflake local authentication using jwt token as a user credential
   */
  SNOWFLAKE_JWT,

  /*
   * Internal authenticator to enable id_token for web browser based authenticator
   */
  ID_TOKEN,

  /*
   * Authenticator to enable token for regular login with mfa
   */
  USERNAME_PASSWORD_MFA,

  /*
   * Authorization code flow with browser popup
   */
  OAUTH_AUTHORIZATION_CODE,

  /*
   * Client credentials flow with clientId and clientSecret as input
   */
  OAUTH_CLIENT_CREDENTIALS,

  /*
   * Authenticator to support PAT created in Snowflake
   */
  PROGRAMMATIC_ACCESS_TOKEN,

  /*
   * Authenticator to support existing authentication by existing AWS/GCP/Azure/OIDC workload identity
   */
  WORKLOAD_IDENTITY
}
