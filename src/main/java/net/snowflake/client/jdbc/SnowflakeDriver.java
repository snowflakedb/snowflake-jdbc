/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import net.snowflake.common.core.ResourceBundleManager;
import net.snowflake.common.core.SqlState;

/**
 * JDBC Driver implementation of Snowflake for production. To use this driver, specify the following
 * URL: jdbc:snowflake://host:port
 *
 * <p>Note: don't add logger to this class since logger init will potentially break driver class
 * loading
 */
public class SnowflakeDriver implements Driver {
  static SnowflakeDriver INSTANCE;

  public static final Properties EMPTY_PROPERTIES = new Properties();
  public static String implementVersion = "3.13.14";

  static int majorVersion = 0;
  static int minorVersion = 0;
  static long patchVersion = 0;

  protected static boolean disableIncidents = false;

  private static boolean disableArrowResultFormat = false;
  private static String disableArrowResultFormatMessage;

  private static final ResourceBundleManager versionResourceBundleManager =
      ResourceBundleManager.getSingleton("net.snowflake.client.jdbc.version");

  static {
    try {
      DriverManager.registerDriver(INSTANCE = new SnowflakeDriver());
    } catch (SQLException ex) {
      throw new IllegalStateException("Unable to register " + SnowflakeDriver.class.getName(), ex);
    }

    initializeArrowSupport();
    /*
     * Get the manifest properties here.
     */
    initializeClientVersionFromManifest();
  }

  /** try to initialize Arrow support if fails, JDBC is going to use the legacy format */
  private static void initializeArrowSupport() {
    try {
      // this is required to enable direct memory usage for Arrow buffers in Java
      System.setProperty("io.netty.tryReflectionSetAccessible", "true");
    } catch (Throwable t) {
      // fail to enable required feature for Arrow
      disableArrowResultFormat = true;
      disableArrowResultFormatMessage = t.getLocalizedMessage();
    }
    disableIllegalReflectiveAccessWarning();
  }

  static void disableIllegalReflectiveAccessWarning() {
    // The netty dependency of arrow will cause an illegal reflective access warning
    // This function try to eliminate the warning by setting
    // jdk.internal.module.IllegalAccessLogger's logger as null
    // Disable this function and manually run arrow tests, e.g. testResultSetMetadata., then
    // the warning can be found in output.
    try {
      Class unsafeClass = Class.forName("sun.misc.Unsafe");
      Field field = unsafeClass.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      Object unsafe = field.get(null);

      Method putObjectVolatile =
          unsafeClass.getDeclaredMethod(
              "putObjectVolatile", Object.class, long.class, Object.class);
      Method staticFieldOffset = unsafeClass.getDeclaredMethod("staticFieldOffset", Field.class);
      Method staticFieldBase = unsafeClass.getDeclaredMethod("staticFieldBase", Field.class);

      Class loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger");
      Field loggerField = loggerClass.getDeclaredField("logger");
      Long loggerOffset = (Long) staticFieldOffset.invoke(unsafe, loggerField);
      Object loggerBase = staticFieldBase.invoke(unsafe, loggerField);
      putObjectVolatile.invoke(unsafe, loggerBase, loggerOffset, null);
    } catch (Throwable ex) {
      // If failed to eliminate warnings, do nothing
    }
  }

  private static void initializeClientVersionFromManifest() {
    /*
     * Get JDBC version numbers from static string in snowflake-jdbc
     */
    try {
      // parse implementation version major.minor.change
      if (implementVersion != null) {
        String[] versionBreakdown = implementVersion.split("\\.");

        if (versionBreakdown.length == 3) {
          majorVersion = Integer.parseInt(versionBreakdown[0]);
          minorVersion = Integer.parseInt(versionBreakdown[1]);
          patchVersion = Long.parseLong(versionBreakdown[2]);
        } else {
          throw new SnowflakeSQLLoggedException(
              null,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              /*session = */ "Invalid Snowflake JDBC Version: " + implementVersion);
        }
      } else {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            /*session = */ null,
            "Snowflake JDBC Version is not set. "
                + "Ensure static version string was initialized.");
      }
    } catch (Throwable ex) {
    }
  }

  /**
   * For testing purposes only- used to compare that JDBC version in pom.xml matches static string
   *
   * @return String with version from pom.xml file
   */
  static String getClientVersionStringFromManifest() {
    return versionResourceBundleManager.getLocalizedMessage("version");
  }

  public static boolean isDisableArrowResultFormat() {
    return disableArrowResultFormat;
  }

  public static String getDisableArrowResultFormatMessage() {
    return disableArrowResultFormatMessage;
  }

  /**
   * Checks whether a given url is in a valid format.
   *
   * <p>The current uri format is: jdbc:snowflake://[host[:port]]
   *
   * <p>jdbc:snowflake:// - run in embedded mode jdbc:snowflake://localhost - connect to localhost
   * default port (8080)
   *
   * <p>jdbc:snowflake://localhost:8080- connect to localhost port 8080
   *
   * @param url url of the database including host and port
   * @return true if the url is valid
   */
  @Override
  public boolean acceptsURL(String url) {
    return SnowflakeConnectString.parse(url, EMPTY_PROPERTIES).isValid();
  }

  /**
   * Connect method
   *
   * @param url jdbc url
   * @param info addition info for passing database/schema names
   * @return connection
   * @throws SQLException if failed to create a snowflake connection
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    SnowflakeConnectString conStr = SnowflakeConnectString.parse(url, info);
    if (!conStr.isValid()) {
      return null;
    }
    return new SnowflakeConnectionV1(url, info);
  }

  @Override
  public int getMajorVersion() {
    return majorVersion;
  }

  @Override
  public int getMinorVersion() {
    return minorVersion;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    DriverPropertyInfo[] retVal;
    if (url == null || url.isEmpty()) {
      retVal = new DriverPropertyInfo[1];
      retVal[0] = new DriverPropertyInfo("serverURL", null);
      retVal[0].description =
          "server URL in form of <protocol>://<host or domain>:<port number>/<path of resource>";
      return retVal;
    }

    Connection con = new SnowflakeConnectionV1(url, info, true);
    List<DriverPropertyInfo> missingProperties =
        ((SnowflakeConnectionV1) con).returnMissingProperties();
    con.close();

    retVal = new DriverPropertyInfo[missingProperties.size()];
    retVal = missingProperties.toArray(retVal);
    return retVal;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  public static boolean isDisableIncidents() {
    return disableIncidents;
  }

  public static void setDisableIncidents(boolean throttleIncidents) {
    SnowflakeDriver.disableIncidents = throttleIncidents;
  }

  public static final void main(String[] args) {
    if (args.length > 0 && "--version".equals(args[0])) {
      Package pkg = Package.getPackage("net.snowflake.client.jdbc");
      if (pkg != null) {
        System.out.println(pkg.getImplementationVersion());
      }
    }
  }
}
