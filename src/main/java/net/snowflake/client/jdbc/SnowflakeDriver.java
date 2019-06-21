/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import net.snowflake.common.core.ResourceBundleManager;
import net.snowflake.common.core.SqlState;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

/**
 * JDBC Driver implementation of Snowflake for production.
 * To use this driver, specify the following URL:
 * jdbc:snowflake://host:port
 * <p>
 * Note: don't add logger to this class since logger init will potentially
 * break driver class loading
 */
public class SnowflakeDriver implements Driver
{
  static SnowflakeDriver INSTANCE;

  private static final
  DriverPropertyInfo[] EMPTY_INFO = new DriverPropertyInfo[0];

  public static String implementVersion = null;

  static int majorVersion = 0;
  static int minorVersion = 0;
  static long patchVersion = 0;

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
          patchVersion = Long.parseLong(versionBreakdown[2]);
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
    // must start with jdbc:
    if (url.startsWith("jdbc:"))
    {
      try
      {
        // URI class gets confused by ":" within jdbc:snowflake, so start after :
        URI uri;
        if (url.contains("http://") || url.contains("https://"))
        {
          uri = URI.create(url.substring(url.indexOf("http")));
        }
        else
        {
          uri = URI.create(url.substring(url.indexOf("snowflake")));
        }
        String scheme = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();
        String queryData = uri.getRawQuery();
        String path = uri.getPath();
        // scheme must be snowflake, else return false
        if(scheme.equals("snowflake") || scheme.equals("http") || scheme.equals("https"))
        {
          // host exists
          if (host != null)
          {
            // query data can only exist if followed by scheme and host
            if (queryData != null)
            {
              String[] params = queryData.split("&");
              for (String p : params)
              {
                String[] keyVals = p.split("=");
                // if it didn't have an = to split in 2, it's invalid. Must follow format a=b
                if (keyVals.length != 2)
                {
                  return false;
                }
                try
                {
                  // decode values and throw exceptions for odd values like %%
                  URLDecoder.decode(keyVals[0], "UTF-8");
                  URLDecoder.decode(keyVals[1], "UTF-8");
                }
                // if there's a decoding exception, return false
                catch (UnsupportedEncodingException e)
                {
                  return false;
                }
              }
              return (params.length > 0);
            }
            else
            {
              if (!path.isEmpty())
              {
                // path must be / with nothing after it
                return url.endsWith("/");
              }
              else if (port != -1)
              {
                // if there's no path, last thing in URL should be port
                return url.endsWith(Integer.toString(port));
              }
              else
              {
                // if there's no path or port, last thing in URL should be host
                return url.endsWith(host);
              }
            }
          }
        }
        //scheme does not equal snowflake
        return false;
      }
      // if URL cannot be made into URI object, it is invalid. Return false.
      catch (Exception e)
      {
        return false;
      }
    }
    return false;
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
