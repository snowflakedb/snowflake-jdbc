package net.snowflake.client.jdbc;

import com.google.common.base.Strings;
import java.io.*;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Created by hyu on 5/11/17. */
public class SnowflakeBasicDataSource implements DataSource, Serializable {
  private static final long serialversionUID = 1L;
  private static final String AUTHENTICATOR_SNOWFLAKE_JWT = "SNOWFLAKE_JWT";
  private static final String AUTHENTICATOR_OAUTH = "OAUTH";
  private String url;

  private String serverName;

  private String user;

  private String password;

  private int portNumber = 0;

  private String authenticator;

  private Properties properties = new Properties();

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeBasicDataSource.class);

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
      properties.put("user", username);
      if (!AUTHENTICATOR_SNOWFLAKE_JWT.equalsIgnoreCase(authenticator)) {
        properties.put("password", password);
      }
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
      return Integer.parseInt(properties.getProperty("loginTimeout"));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    properties.put("loginTimeout", Integer.toString(seconds));
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
    properties.put("db", databaseName);
  }

  public void setSchema(String schema) {
    properties.put("schema", schema);
  }

  public void setWarehouse(String warehouse) {
    properties.put("warehouse", warehouse);
  }

  public void setRole(String role) {
    properties.put("role", role);
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
    this.properties.put("account", account);
  }

  public void setSsl(boolean ssl) {
    this.properties.put("ssl", String.valueOf(ssl));
  }

  public void setAuthenticator(String authenticator) {
    this.authenticator = authenticator;
    this.properties.put("authenticator", authenticator);
  }

  public void setOauthToken(String oauthToken) {
    this.setAuthenticator(AUTHENTICATOR_OAUTH);
    this.properties.put("token", oauthToken);
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
    this.properties.put("privateKey", privateKey);
  }

  public void setPrivateKeyFile(String location, String password) {
    this.setAuthenticator(AUTHENTICATOR_SNOWFLAKE_JWT);
    this.properties.put("private_key_file", location);
    if (!Strings.isNullOrEmpty(password)) {
      this.properties.put("private_key_file_pwd", password);
    }
  }

  public void setTracing(String tracing) {
    this.properties.put("tracing", tracing);
  }
}
