/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.ResourceBundleManager;
import net.snowflake.client.core.EventHandler;
import net.snowflake.client.core.EventUtil;
import net.snowflake.common.core.SqlState;

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
 * <p>
 * Note: don't add logger to this class since logger init will potentially
 * break driver class loading
 *
 * @author jhuang
 */
public class SnowflakeDriver implements Driver
{
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

      // parse implementation version major.minor.change
      if (implementVersion != null)
      {
        String[] versionBreakdown = implementVersion.split("\\.");

        if (versionBreakdown.length == 3)
        {
          majorVersion = Integer.parseInt(versionBreakdown[0]);
          minorVersion = Integer.parseInt(versionBreakdown[1]);
          changeVersion = Long.parseLong(versionBreakdown[2]);
        }
        else
        {
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                          ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                          "Invalid Snowflake JDBC Version: " + implementVersion);
        }
      }
      else
      {
        throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                        ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                        "Snowflake JDBC Version is not set. " +
                                        "Ensure version.properties is included.");
      }
    }
    catch (Throwable ex)
    {
    }
  }

  /**
   * Checks whether a given url is in a valid format.
   * <p>
   * The current uri format is: jdbc:snowflake://[host[:port]]
   * <p>
   * jdbc:snowflake:// - run in embedded mode jdbc:snowflake://localhost -
   * connect to localhost default port (8080)
   * <p>
   * jdbc:snowflake://localhost:8080- connect to localhost port 8080
   *
   * @param url url of the database including host and port
   * @return true if the url is valid
   */
  @Override
  public boolean acceptsURL(String url)
  {
    if (url == null)
    {
      return false;
    }

    return url.indexOf("/?") > 0 ?
           Pattern.matches(JDBC_PROTOCOL_REGEX,
                           url.substring(0, url.indexOf("/?"))) :
           Pattern.matches(JDBC_PROTOCOL_REGEX, url);
  }

  /**
   * Connect method
   *
   * @param url  jdbc url
   * @param info addition info for passing database/schema names
   * @return connection
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
    SnowflakeDriver.disableIncidents =
        throttleIncidents;
  }

  public final static void main(String[] args)
  {
    if (args.length > 0 && "--version".equals(args[0]))
    {
      Package pkg = Package.getPackage("net.snowflake.client.jdbc");
      if (pkg != null)
      {
        System.out.println(pkg.getImplementationVersion());
      }
    }
  }
}
