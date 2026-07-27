package net.snowflake.client.api.datasource;

import java.security.PrivateKey;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import net.snowflake.client.api.http.HttpHeadersCustomizer;

/**
 * Snowflake-specific extension of {@link javax.sql.DataSource} that provides configuration methods
 * for Snowflake JDBC connections.
 *
 * <p>Use {@link SnowflakeDataSourceFactory} to create instances of this interface.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * SnowflakeDataSource ds = SnowflakeDataSourceFactory.createDataSource();
 * ds.setAccount("myaccount");
 * ds.setUser("myuser");
 * ds.setPassword("mypassword");
 * ds.setDatabase("mydb");
 * ds.setSchema("myschema");
 * ds.setWarehouse("mywh");
 *
 * try (Connection conn = ds.getConnection()) {
 *   // use connection
 * }
 * }</pre>
 *
 * @see SnowflakeDataSourceFactory
 */
public interface SnowflakeDataSource extends DataSource {

  /** Sets the JDBC URL for the connection. */
  void setUrl(String url);

  /** Sets the database name. */
  void setDatabaseName(String databaseName);

  /** Sets the schema name. */
  void setSchema(String schema);

  /** Sets the warehouse name. */
  void setWarehouse(String warehouse);

  /** Sets the role name. */
  void setRole(String role);

  /** Sets the user name. */
  void setUser(String user);

  /** Sets the server name (hostname). */
  void setServerName(String serverName);

  /** Sets the password. */
  void setPassword(String password);

  /** Sets the port number. */
  void setPortNumber(int portNumber);

  /** Sets the account identifier. */
  void setAccount(String account);

  /** Sets whether to use SSL (default: true). */
  void setSsl(boolean ssl);

  /** Sets the authenticator type (e.g., "snowflake", "externalbrowser", "oauth"). */
  void setAuthenticator(String authenticator);

  /** Sets the token for OAuth/PAT authentication. */
  void setToken(String token);

  /** Gets the JDBC URL. */
  String getUrl();

  /** Sets the private key for key-pair authentication. */
  void setPrivateKey(PrivateKey privateKey);

  /** Sets the private key file location and optional password for key-pair authentication. */
  void setPrivateKeyFile(String location, String password);

  /** Sets the Base64-encoded private key and optional password for key-pair authentication. */
  void setPrivateKeyBase64(String privateKeyBase64, String password);

  /** Sets the tracing level. */
  void setTracing(String tracing);

  /** Gets the connection properties. */
  Properties getProperties();

  /** Sets whether to allow underscores in hostnames. */
  void setAllowUnderscoresInHost(boolean allowUnderscoresInHost);

  /** Sets whether to disable GCS default credentials. */
  void setDisableGcsDefaultCredentials(boolean isGcsDefaultCredentialsDisabled);

  /** Sets whether to disable SAML URL validation. */
  void setDisableSamlURLCheck(boolean disableSamlURLCheck);

  /** Sets the passcode for MFA authentication. */
  void setPasscode(String passcode);

  /** Sets whether the passcode is included in the password for MFA authentication. */
  void setPasscodeInPassword(boolean isPasscodeInPassword);

  /** Sets whether to disable SOCKS proxy. */
  void setDisableSocksProxy(boolean ignoreJvmSocksProxy);

  /** Sets non-proxy hosts pattern. */
  void setNonProxyHosts(String nonProxyHosts);

  /** Sets the proxy host. */
  void setProxyHost(String proxyHost);

  /** Sets the proxy password. */
  void setProxyPassword(String proxyPassword);

  /** Sets the proxy port. */
  void setProxyPort(int proxyPort);

  /** Sets the proxy protocol (e.g., "http", "https"). */
  void setProxyProtocol(String proxyProtocol);

  /** Sets the proxy user. */
  void setProxyUser(String proxyUser);

  /** Sets whether to use a proxy. */
  void setUseProxy(boolean useProxy);

  /** Sets the network timeout in seconds. */
  void setNetworkTimeout(int networkTimeoutSeconds);

  /** Sets the query timeout in seconds. */
  void setQueryTimeout(int queryTimeoutSeconds);

  /** Sets the application name. */
  void setApplication(String application);

  /** Sets the client configuration file path. */
  void setClientConfigFile(String clientConfigFile);

  /** Sets whether to enable pattern search in metadata queries. */
  void setEnablePatternSearch(boolean enablePatternSearch);

  /** Sets whether to enable PUT/GET commands. */
  void setEnablePutGet(boolean enablePutGet);

  /** Sets whether to treat Arrow DECIMAL columns as INT. */
  void setArrowTreatDecimalAsInt(boolean treatDecimalAsInt);

  /** Sets the maximum number of HTTP retries. */
  void setMaxHttpRetries(int maxHttpRetries);

  /** Sets whether OCSP checking should fail open. */
  void setOcspFailOpen(boolean ocspFailOpen);

  /** Sets the maximum number of PUT/GET retries. */
  void setPutGetMaxRetries(int putGetMaxRetries);

  /** Sets whether strings are quoted in column definitions. */
  void setStringsQuotedForColumnDef(boolean stringsQuotedForColumnDef);

  /** Sets whether to enable diagnostics. */
  void setEnableDiagnostics(boolean enableDiagnostics);

  /** Sets the diagnostics allowlist file path. */
  void setDiagnosticsAllowlistFile(String diagnosticsAllowlistFile);

  /** Sets the default date format with timezone for JDBC. */
  void setJDBCDefaultFormatDateWithTimezone(Boolean jdbcDefaultFormatDateWithTimezone);

  /** Sets whether getDate should use null timezone. */
  void setGetDateUseNullTimezone(Boolean getDateUseNullTimezone);

  /** Sets whether to enable client-side MFA token request. */
  void setEnableClientRequestMfaToken(boolean enableClientRequestMfaToken);

  /** Sets whether to enable client-side storage of temporary credentials. */
  void setEnableClientStoreTemporaryCredential(boolean enableClientStoreTemporaryCredential);

  /** Sets the browser response timeout in seconds for external browser authentication. */
  void setBrowserResponseTimeout(int seconds);

  /** Sets custom HTTP header customizers. */
  void setHttpHeadersCustomizers(List<HttpHeadersCustomizer> httpHeadersCustomizers);
}
