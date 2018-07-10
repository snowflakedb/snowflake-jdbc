/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.ResourceBundleManager;
import net.snowflake.client.core.EventHandler;
import net.snowflake.client.core.EventUtil;
import net.snowflake.common.core.SqlState;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Pattern;

import net.snowflake.client.log.*;

/**
 * JDBC Driver implementation of Snowflake for production.
 * To use this driver, specify the following URL:
 * jdbc:snowflake://host:port
 *
 * @author jhuang
 */
public class SnowflakeDriver implements Driver
{
  public static Handler fileHandler = null;

  static final
  SFLogger logger = SFLoggerFactory.getLogger(SnowflakeDriver.class);

  private static final String JDBC_PROTOCOL = "jdbc:snowflake://";

  // pattern for jdbc:snowflake://[host[:port]]/?q1=v1&q2=v2...
  private static final String JDBC_PROTOCOL_REGEX =
  "jdbc:snowflake://([a-zA-Z_\\-0-9\\.]+(:\\d+)?)?"
  + "(/?(\\?\\w+=\\w+)?(\\&\\w+=\\w+)*)?";

  public static SnowflakeDriver INSTANCE = null;

  private static final
  DriverPropertyInfo[] EMPTY_INFO = new DriverPropertyInfo[0];

  public static String implementVersion = null;

  public static int majorVersion = 0;
  public static int minorVersion = 0;
  public static long changeVersion = 0;

  protected static boolean disableIncidents = false;

  private static final ResourceBundleManager versionResourceBundleManager
      = ResourceBundleManager.getSingleton("net.snowflake.client.jdbc.version");

  static
  {
    try
    {
      DriverManager.registerDriver(INSTANCE = new SnowflakeDriver());
    }
    catch (SQLException ex)
    {
      throw new IllegalStateException("Unable to register "
              + SnowflakeDriver.class.getName(), ex);
    }

    try
    {
      String loggerImpl = System.getProperty("net.snowflake.jdbc.loggerImpl");
      EventUtil.initEventHandlerInstance(1000, 10000);
      EventHandler eventHandler = EventUtil.getEventHandlerInstance();

      if (logger instanceof JDK14Logger)
      {
        eventHandler.setLevel(Level.ALL);
        eventHandler.setFormatter(new SimpleFormatter());

        JDK14Logger.addHandler(eventHandler);
      }

      // if this system property is not set and logger is still jdk logger, then we are
      // assuming the customer is using the old logging config
      if (loggerImpl == null && (logger instanceof JDK14Logger))
      {
        String defaultLogSizeVal = System.getProperty("snowflake.jdbc.log.size");
        String defaultLogCountVal = System.getProperty("snowflake.jdbc.log.count");

        // default log size to 1 GB
        int logSize = 1000000000;

        // default number of log files to rotate to 2
        int logCount = 2;

        if (defaultLogSizeVal != null) {
          try {
            logSize = Integer.parseInt(defaultLogSizeVal);
          } catch (Exception ex) {
            ;
          }
        }

        if (defaultLogCountVal != null) {
          try {
            logCount = Integer.parseInt(defaultLogCountVal);
          } catch (Exception ex) {
            ;
          }
        }

        fileHandler = new FileHandler("%t/snowflake_jdbc%u.log",
            logSize, logCount, true);

        String defaultLevelVal = System.getProperty("snowflake.jdbc.log.level");

        Level defaultLevel = Level.WARNING;

        if (defaultLevelVal != null) {
          defaultLevel = Level.parse(defaultLevelVal.toUpperCase());

          if (defaultLevel == null)
            defaultLevel = Level.WARNING;
        }

        fileHandler.setLevel(Level.ALL);
        fileHandler.setFormatter(new SFFormatter());

        // set default level and add handler for snowflake logger
        JDK14Logger.setLevel(defaultLevel);
        JDK14Logger.addHandler(SnowflakeDriver.fileHandler);

        Logger snowflakeLoggerInformaticaV1 = Logger.getLogger(
            SFFormatter.INFORMATICA_V1_CLASS_NAME_PREFIX);
        snowflakeLoggerInformaticaV1.setLevel(defaultLevel);
        snowflakeLoggerInformaticaV1.addHandler(SnowflakeDriver.fileHandler);
        snowflakeLoggerInformaticaV1.addHandler(eventHandler);
      }

      logger.debug("registered driver");
    }
    catch (Exception ex)
    {
      System.err.println("Unable to set up logging");
      ex.printStackTrace();
    }

    /*
     * Get the manifest properties here.
     */

    initializeClientVersionFromManifest();
  }

  static private void initializeClientVersionFromManifest()
  {
    /*
     * Get JDBC version numbers from version.properties in snowflake-jdbc
     */
    try
    {
      implementVersion = versionResourceBundleManager.getLocalizedMessage("version");

      logger.debug("implement version: {}", implementVersion);

      // parse implementation version major.minor.change
      if (implementVersion != null)
      {
        String[] versionBreakdown = implementVersion.split("\\.");

        if (versionBreakdown != null && versionBreakdown.length == 3)
        {
          majorVersion = Integer.parseInt(versionBreakdown[0]);
          minorVersion = Integer.parseInt(versionBreakdown[1]);
          changeVersion = Long.parseLong(versionBreakdown[2]);
        }
        else
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "Invalid implementation version: " + implementVersion);
      }
      else
        throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            "Null implementation version");

      logger.debug("implementation_version = {}", implementVersion);
      logger.debug("major version = {}", majorVersion);
      logger.debug("minor version = {}", minorVersion);
      logger.debug("change version = {}", changeVersion);
    }
    catch (Exception ex)
    {
      logger.error("Exception encountered when retrieving client "
          + "version attributes: {}", ex.getMessage());
    }

    /*
     * Get the svn revision here.
     */
    try
    {
      if (SnowflakeConnectionV1.class.getProtectionDomain() == null ||
          SnowflakeConnectionV1.class.getProtectionDomain().getCodeSource()
          == null ||
          SnowflakeConnectionV1.class.getProtectionDomain().getCodeSource().
                  getLocation() == null)
      {
        logger.debug("Couldn't get code source location");
        return;
      }

      URI jarURI =
              SnowflakeConnectionV1.class.getProtectionDomain().getCodeSource().
                      getLocation().toURI();

      logger.debug("jar uri: {}, to_url: {}", jarURI.getPath(),
                             jarURI.toURL());

      // if code source is not from a jar, there will be no manifest, so skip
      // the svn revision setup
      if (jarURI == null || jarURI.getPath() == null ||
          !jarURI.getPath().endsWith(".jar"))
      {
        logger.debug("couldn't determine code source");
        return;
      }

      URL jarURL = jarURI.toURL();

      if (jarURL == null)
      {
        logger.debug("null jar URL");
        return;
      }

      InputStream is = jarURL.openStream();

      if (is == null)
      {
        logger.debug("Can not open snowflake-jdbc.jar: " +
                               jarURI.getPath());
        return;
      }

      JarInputStream jarStream = new JarInputStream(is);
      Manifest mf = jarStream.getManifest();

      if (mf == null)
      {
        logger.debug("Manifest not found from snowflake-jdbc.jar");
        return;
      }

      Attributes mainAttribs = mf.getMainAttributes();

      if (mainAttribs == null)
      {
        logger.debug(
                  "mainAttribs null from the manifest in snowflake-jdbc.jar");
        return;
      }

    }
    catch (Throwable ex)
    {
      logger.debug("Exception encountered when retrieving client "
                               + "svn revision attribute from manifest: "
                               + ex.getMessage());
    }
  }

  /**
   * Checks whether a given url is in a valid format.
   *
   * The current uri format is: jdbc:snowflake://[host[:port]]
   *
   * jdbc:snowflake:// - run in embedded mode jdbc:snowflake://localhost -
   * connect to localhost default port (8080)
   *
   * jdbc:snowflake://localhost:8080- connect to localhost port 8080
   *
   * @param url
   *   url of the database including host and port
   * @return
   *   true if the url is valid
   * @throws SQLException if failed to accept url
   */
  @Override
  public boolean acceptsURL(String url) throws SQLException
  {
    if (url == null)
      return false;

    return url.indexOf("/?") > 0?
           Pattern.matches(JDBC_PROTOCOL_REGEX,
                           url.substring(0, url.indexOf("/?"))):
           Pattern.matches(JDBC_PROTOCOL_REGEX, url);
  }

  /**
   * Connect method
   *
   * @param url
   *   jdbc url
   * @param info
   *   addition info for passing database/schema names
   * @return
   *   connection
   * @throws SQLException if failed to create a snowflake connection
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    if (acceptsURL(url))
    {
      return new SnowflakeConnectionV1(url, info);
    }
    return null;
  }

  @Override
  public int getMajorVersion()
  {
    return majorVersion;
  }

  @Override
  public int getMinorVersion()
  {
    return minorVersion;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
          throws SQLException
  {
    return EMPTY_INFO;
  }

  @Override
  public boolean jdbcCompliant()
  {
    return false;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
  {
    return null;
  }

  public static boolean isDisableIncidents()
  {
    return disableIncidents;
  }

  public static void setDisableIncidents(
      boolean throttleIncidents)
  {
    if (throttleIncidents)
      logger.trace("setting throttle incidents");

    SnowflakeDriver.disableIncidents =
        throttleIncidents;
  }

}
