package net.snowflake.client.api.auth;

/**
 * Enumeration of authentication methods supported by the Snowflake JDBC driver.
 *
 * <p>This enum defines the various authentication mechanisms that can be used to establish a
 * connection to Snowflake. The authenticator type is specified via the connection property {@code
 * authenticator}.
 *
 * <h2 id="usage-example">Usage Example</h2>
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.put("user", "myuser");
 * props.put("authenticator", "EXTERNAL_BROWSER");
 * Connection conn = DriverManager.getConnection(url, props);
 * }</pre>
 *
 */
public enum AuthenticatorType {
  /** Regular login with username and password via Snowflake, may or may not have MFA */
  SNOWFLAKE,

  /** Federated authentication with OKTA as identity provider */
  OKTA,

  /** Web-browser-based authenticator for SAML 2.0 compliant service/application */
  EXTERNAL_BROWSER,

  /** OAuth 2.0 authentication flow */
  OAUTH,

  /** Snowflake JWT token authentication using a private key */
  SNOWFLAKE_JWT,

  /** Internal authenticator to enable id_token for web browser based authentication */
  ID_TOKEN,

  /** Authenticator to enable token for regular login with MFA */
  USERNAME_PASSWORD_MFA,

  /** OAuth authorization code flow with browser popup */
  OAUTH_AUTHORIZATION_CODE,

  /** OAuth client credentials flow with clientId and clientSecret */
  OAUTH_CLIENT_CREDENTIALS,

  /** Programmatic Access Token (PAT) authentication created in Snowflake */
  PROGRAMMATIC_ACCESS_TOKEN,

  /**
   * Workload identity authentication using existing AWS/GCP/Azure/OIDC workload identity
   * credentials
   */
  WORKLOAD_IDENTITY
}
