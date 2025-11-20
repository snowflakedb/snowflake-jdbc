package net.snowflake.client.api.driver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.driver.AutoConfigurationHelper;
import net.snowflake.client.internal.driver.ConnectionFactory;
import net.snowflake.client.internal.driver.DriverInitializer;
import net.snowflake.client.internal.driver.DriverVersion;
import net.snowflake.client.internal.jdbc.SnowflakeConnectString;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.common.core.ResourceBundleManager;

/**
 * JDBC Driver implementation for Snowflake.
 *
 * <p>To use this driver, specify the following URL formats:
 *
 * <ul>
 *   <li>{@code jdbc:snowflake://host:port} - Standard connection
 *   <li>{@code jdbc:snowflake:auto} - Auto-configuration from connections.toml
 * </ul>
 *
 * <p>The driver is automatically registered with {@link DriverManager} when loaded.
 *
 * @see java.sql.Driver
 */
public class SnowflakeDriver implements Driver {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeDriver.class);

  public static final SnowflakeDriver INSTANCE;
  public static final Properties EMPTY_PROPERTIES = new Properties();

  private static final DriverVersion VERSION = DriverVersion.getInstance();

  static final ResourceBundleManager versionResourceBundleManager =
      ResourceBundleManager.getSingleton("net.snowflake.client.jdbc.version");

  static {
    try {
      DriverManager.registerDriver(INSTANCE = new SnowflakeDriver());
      logger.info("Snowflake JDBC Driver {} registered successfully", VERSION.getFullVersion());
    } catch (SQLException ex) {
      throw new IllegalStateException("Unable to register " + SnowflakeDriver.class.getName(), ex);
    }

    // Perform all driver initialization (Arrow, security, telemetry, etc.)
    DriverInitializer.initialize();
  }

  /**
   * Checks whether a given URL is in a valid Snowflake JDBC format.
   *
   * <p>Valid formats:
   *
   * <ul>
   *   <li>{@code jdbc:snowflake://host[:port]} - Standard connection
   *   <li>{@code jdbc:snowflake:auto} - Auto-configuration
   * </ul>
   *
   * @param url the database URL
   * @return true if the URL is valid and accepted by this driver
   */
  @Override
  public boolean acceptsURL(String url) {
    if (AutoConfigurationHelper.isAutoConfigurationUrl(url)) {
      return true;
    }
    return SnowflakeConnectString.parse(url, EMPTY_PROPERTIES).isValid();
  }

  /**
   * Establishes a connection to the Snowflake database.
   *
   * @param url the database URL
   * @param info additional connection properties
   * @return a Connection object, or null if the URL is not accepted
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return ConnectionFactory.createConnection(url, info);
  }

  /**
   * Establishes a connection using auto-configuration.
   *
   * @return a Connection object
   * @throws SQLException if a database access error occurs
   */
  public Connection connect() throws SQLException {
    return ConnectionFactory.createConnectionWithAutoConfig();
  }

  @Override
  public int getMajorVersion() {
    return VERSION.getMajor();
  }

  @Override
  public int getMinorVersion() {
    return VERSION.getMinor();
  }

  /**
   * Gets the driver patch version number. This is not part of the standard JDBC Driver interface,
   * but is needed for full version information.
   *
   * @return driver patch version number
   */
  public long getPatchVersion() {
    return VERSION.getPatch();
  }

  /**
   * Gets driver version information for telemetry and logging.
   *
   * @return full version string (e.g., "4.0.0")
   */
  public static String getImplementationVersion() {
    return VERSION.getFullVersion();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    if (url == null || url.isEmpty()) {
      DriverPropertyInfo[] result = new DriverPropertyInfo[1];
      result[0] = new DriverPropertyInfo("serverURL", null);
      result[0].description =
          "server URL in form of <protocol>://<host or domain>:<port number>/<path of resource>";
      return result;
    }

    try (SnowflakeConnectionImpl con = new SnowflakeConnectionImpl(url, info, true)) {
      List<DriverPropertyInfo> missingProperties = con.returnMissingProperties();
      return missingProperties.toArray(new DriverPropertyInfo[0]);
    }
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    return null;
  }
}
