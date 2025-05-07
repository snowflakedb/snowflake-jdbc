package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SnowflakeBasicDataSource implements DataSource, Serializable {
  private static final long serialversionUID = 1L;
  private static final String AUTHENTICATOR_SNOWFLAKE_JWT = "SNOWFLAKE_JWT";
  private static final String AUTHENTICATOR_OAUTH = "OAUTH";

  private static final String AUTHENTICATOR_EXTERNAL_BROWSER = "EXTERNALBROWSER";

  private static final String AUTHENTICATOR_USERNAME_PASSWORD_MFA = "USERNAME_PASSWORD_MFA";

  private String url;

  private String serverName;

  private String user;

  private String password;

  private int portNumber = 0;

  private String authenticator;

  private Properties properties = new Properties();

  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeBasicDataSource.class);

  static {
    try {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Unable to load "
              + "net.snowflake.client.jdbc.SnowflakeDriver. "
              + "Please check if you have proper Snowflake JDBC "
              + "Driver jar on the classpath",
          e);
    }
  }

  private void writeObjectHelper(ObjectOutputStream out) throws IOException {
    out.writeObject(url);
    out.writeObject(serverName);
    out.writeObject(user);
    out.writeObject(password);
    out.writeObject(portNumber);
    out.writeObject(authenticator);
    out.writeObject(properties);
  }

  private void readObjectHelper(ObjectInputStream in) throws IOException, ClassNotFoundException {
    url = (String) in.readObject();
    serverName = (String) in.readObject();
    user = (String) in.readObject();
    password = (String) in.readObject();
    portNumber = (int) in.readObject();
    authenticator = (String) in.readObject();
    properties = (Properties) in.readObject();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeObjectHelper(out);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    readObjectHelper(in);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(user, password);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    if (!AUTHENTICATOR_OAUTH.equalsIgnoreCase(
        authenticator)) { // For OAuth, no username is required
      if (username == null) {
        throw new SnowflakeSQLException(
            "Cannot create connection because username is missing in DataSource properties.");
      }
      properties.put(SFSessionProperty.USER.getPropertyKey(), username);
    }

    // The driver needs password for OAUTH as part of SNOW-533673 feature request.
    if (!AUTHENTICATOR_SNOWFLAKE_JWT.equalsIgnoreCase(authenticator)
        && !AUTHENTICATOR_EXTERNAL_BROWSER.equalsIgnoreCase(authenticator)) {
      if (password == null) {
        throw new SnowflakeSQLException(
            "Cannot create connection because password is missing in DataSource properties.");
      }
      properties.put(SFSessionProperty.PASSWORD.getPropertyKey(), password);
    }

    try {
      Connection con = SnowflakeDriver.INSTANCE.connect(getUrl(), properties);
      logger.trace("Created a connection for {} at {}", user, (ArgSupplier) this::getUrl);
      return con;
    } catch (SQLException e) {
      logger.error("Failed to create a connection for {} at {}: {}", user, getUrl(), e);
      throw e;
    }
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    try {
      return Integer.parseInt(
          properties.getProperty(SFSessionProperty.LOGIN_TIMEOUT.getPropertyKey()));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    properties.put(SFSessionProperty.LOGIN_TIMEOUT.getPropertyKey(), Integer.toString(seconds));
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setDatabaseName(String databaseName) {
    properties.put(SFSessionProperty.DATABASE.getPropertyKey(), databaseName);
  }

  public void setSchema(String schema) {
    properties.put(SFSessionProperty.SCHEMA.getPropertyKey(), schema);
  }

  public void setWarehouse(String warehouse) {
    properties.put(SFSessionProperty.WAREHOUSE.getPropertyKey(), warehouse);
  }

  public void setRole(String role) {
    properties.put(SFSessionProperty.ROLE.getPropertyKey(), role);
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setPortNumber(int portNumber) {
    this.portNumber = portNumber;
  }

  public void setAccount(String account) {
    this.properties.put(SFSessionProperty.ACCOUNT.getPropertyKey(), account);
  }

  public void setSsl(boolean ssl) {
    this.properties.put("ssl", String.valueOf(ssl));
  }

  public void setAuthenticator(String authenticator) {
    this.authenticator = authenticator;
    this.properties.put(SFSessionProperty.AUTHENTICATOR.getPropertyKey(), authenticator);
  }

  public void setOauthToken(String oauthToken) {
    this.setAuthenticator(AUTHENTICATOR_OAUTH);
    this.properties.put(SFSessionProperty.TOKEN.getPropertyKey(), oauthToken);
  }

  public String getUrl() {
    if (url != null) {
      return url;
    } else {
      // generate url;
      StringBuilder url = new StringBuilder(100);
      url.append("jdbc:snowflake://");
      url.append(serverName);
      if (portNumber != 0) {
        url.append(":").append(portNumber);
      }

      return url.toString();
    }
  }

  public void setPrivateKey(PrivateKey privateKey) {
    this.setAuthenticator(AUTHENTICATOR_SNOWFLAKE_JWT);
    this.properties.put(SFSessionProperty.PRIVATE_KEY.getPropertyKey(), privateKey);
  }

  public void setPrivateKeyFile(String location, String password) {
    this.setAuthenticator(AUTHENTICATOR_SNOWFLAKE_JWT);
    this.properties.put(SFSessionProperty.PRIVATE_KEY_FILE.getPropertyKey(), location);
    if (!isNullOrEmpty(password)) {
      this.properties.put(SFSessionProperty.PRIVATE_KEY_PWD.getPropertyKey(), password);
    }
  }

  public void setPrivateKeyBase64(String privateKeyBase64, String password) {
    this.setAuthenticator(AUTHENTICATOR_SNOWFLAKE_JWT);
    this.properties.put(SFSessionProperty.PRIVATE_KEY_BASE64.getPropertyKey(), privateKeyBase64);
    if (!isNullOrEmpty(password)) {
      this.properties.put(SFSessionProperty.PRIVATE_KEY_PWD.getPropertyKey(), password);
    }
  }

  public void setTracing(String tracing) {
    this.properties.put(SFSessionProperty.TRACING.getPropertyKey(), tracing);
  }

  protected Properties getProperties() {
    return this.properties;
  }

  public void setAllowUnderscoresInHost(boolean allowUnderscoresInHost) {
    this.properties.put(
        SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(),
        String.valueOf(allowUnderscoresInHost));
  }

  public void setDisableGcsDefaultCredentials(boolean isGcsDefaultCredentialsDisabled) {
    this.properties.put(
        SFSessionProperty.DISABLE_GCS_DEFAULT_CREDENTIALS.getPropertyKey(),
        String.valueOf(isGcsDefaultCredentialsDisabled));
  }

  public void setDisableSamlURLCheck(boolean disableSamlURLCheck) {
    this.properties.put(
        SFSessionProperty.DISABLE_SAML_URL_CHECK.getPropertyKey(),
        String.valueOf(disableSamlURLCheck));
  }

  public void setPasscode(String passcode) {
    this.setAuthenticator(AUTHENTICATOR_USERNAME_PASSWORD_MFA);
    this.properties.put(SFSessionProperty.PASSCODE.getPropertyKey(), passcode);
  }

  public void setPasscodeInPassword(boolean isPasscodeInPassword) {
    this.properties.put(
        SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey(),
        String.valueOf(isPasscodeInPassword));
    if (isPasscodeInPassword) {
      this.setAuthenticator(AUTHENTICATOR_USERNAME_PASSWORD_MFA);
    }
  }

  public void setDisableSocksProxy(boolean ignoreJvmSocksProxy) {
    this.properties.put(
        SFSessionProperty.DISABLE_SOCKS_PROXY.getPropertyKey(),
        String.valueOf(ignoreJvmSocksProxy));
  }

  public void setNonProxyHosts(String nonProxyHosts) {
    this.properties.put(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey(), nonProxyHosts);
  }

  public void setProxyHost(String proxyHost) {
    this.properties.put(SFSessionProperty.PROXY_HOST.getPropertyKey(), proxyHost);
  }

  public void setProxyPassword(String proxyPassword) {
    this.properties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), proxyPassword);
  }

  public void setProxyPort(int proxyPort) {
    this.properties.put(SFSessionProperty.PROXY_PORT.getPropertyKey(), Integer.toString(proxyPort));
  }

  public void setProxyProtocol(String proxyProtocol) {
    this.properties.put(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey(), proxyProtocol);
  }

  public void setProxyUser(String proxyUser) {
    this.properties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), proxyUser);
  }

  public void setUseProxy(boolean useProxy) {
    this.properties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), String.valueOf(useProxy));
  }

  public void setNetworkTimeout(int networkTimeoutSeconds) {
    this.properties.put(
        SFSessionProperty.NETWORK_TIMEOUT.getPropertyKey(),
        Integer.toString(networkTimeoutSeconds));
  }

  public void setQueryTimeout(int queryTimeoutSeconds) {
    this.properties.put(
        SFSessionProperty.QUERY_TIMEOUT.getPropertyKey(), Integer.toString(queryTimeoutSeconds));
  }

  public void setApplication(String application) {
    this.properties.put(SFSessionProperty.APPLICATION.getPropertyKey(), application);
  }

  public void setClientConfigFile(String clientConfigFile) {
    this.properties.put(SFSessionProperty.CLIENT_CONFIG_FILE.getPropertyKey(), clientConfigFile);
  }

  public void setEnablePatternSearch(boolean enablePatternSearch) {
    this.properties.put(
        SFSessionProperty.ENABLE_PATTERN_SEARCH.getPropertyKey(),
        String.valueOf(enablePatternSearch));
  }

  public void setEnablePutGet(boolean enablePutGet) {
    this.properties.put(
        SFSessionProperty.ENABLE_PUT_GET.getPropertyKey(), String.valueOf(enablePutGet));
  }

  public void setArrowTreatDecimalAsInt(boolean treatDecimalAsInt) {
    this.properties.put(
        SFSessionProperty.JDBC_ARROW_TREAT_DECIMAL_AS_INT.getPropertyKey(),
        String.valueOf(treatDecimalAsInt));
  }

  public void setMaxHttpRetries(int maxHttpRetries) {
    this.properties.put(
        SFSessionProperty.MAX_HTTP_RETRIES.getPropertyKey(), Integer.toString(maxHttpRetries));
  }

  public void setOcspFailOpen(boolean ocspFailOpen) {
    this.properties.put(
        SFSessionProperty.OCSP_FAIL_OPEN.getPropertyKey(), String.valueOf(ocspFailOpen));
  }

  public void setPutGetMaxRetries(int putGetMaxRetries) {
    this.properties.put(
        SFSessionProperty.PUT_GET_MAX_RETRIES.getPropertyKey(), Integer.toString(putGetMaxRetries));
  }

  public void setStringsQuotedForColumnDef(boolean stringsQuotedForColumnDef) {
    this.properties.put(
        SFSessionProperty.STRINGS_QUOTED.getPropertyKey(),
        String.valueOf(stringsQuotedForColumnDef));
  }

  public void setEnableDiagnostics(boolean enableDiagnostics) {
    this.properties.put(
        SFSessionProperty.ENABLE_DIAGNOSTICS.getPropertyKey(), String.valueOf(enableDiagnostics));
  }

  public void setDiagnosticsAllowlistFile(String diagnosticsAllowlistFile) {
    this.properties.put(
        SFSessionProperty.DIAGNOSTICS_ALLOWLIST_FILE.getPropertyKey(), diagnosticsAllowlistFile);
  }

  public void setJDBCDefaultFormatDateWithTimezone(Boolean jdbcDefaultFormatDateWithTimezone) {
    this.properties.put(
        "JDBC_DEFAULT_FORMAT_DATE_WITH_TIMEZONE", jdbcDefaultFormatDateWithTimezone);
  }

  public void setGetDateUseNullTimezone(Boolean getDateUseNullTimezone) {
    this.properties.put("JDBC_GET_DATE_USE_NULL_TIMEZONE", getDateUseNullTimezone);
  }

  public void setEnableClientRequestMfaToken(boolean enableClientRequestMfaToken) {
    this.setAuthenticator(AUTHENTICATOR_USERNAME_PASSWORD_MFA);
    this.properties.put(
        SFSessionProperty.ENABLE_CLIENT_REQUEST_MFA_TOKEN.getPropertyKey(),
        enableClientRequestMfaToken);
  }

  public void setEnableClientStoreTemporaryCredential(
      boolean enableClientStoreTemporaryCredential) {
    this.setAuthenticator(AUTHENTICATOR_EXTERNAL_BROWSER);
    this.properties.put(
        SFSessionProperty.ENABLE_CLIENT_STORE_TEMPORARY_CREDENTIAL.getPropertyKey(),
        enableClientStoreTemporaryCredential);
  }

  public void setBrowserResponseTimeout(int seconds) {
    this.setAuthenticator(AUTHENTICATOR_EXTERNAL_BROWSER);
    this.properties.put("BROWSER_RESPONSE_TIMEOUT", Integer.toString(seconds));
  }
}
