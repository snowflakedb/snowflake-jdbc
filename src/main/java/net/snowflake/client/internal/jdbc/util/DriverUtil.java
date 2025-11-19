package net.snowflake.client.internal.jdbc.util;

import static net.snowflake.client.api.driver.SnowflakeDriver.implementVersion;

public class DriverUtil {
  /**
   * Utility method to verify if the standard or fips snowflake-jdbc driver is being used.
   *
   * @return the title of the implementation, null is returned if it is not known.
   */
  static String getImplementationTitle() {
    Package pkg = Package.getPackage("net.snowflake.client.internal.jdbc");
    return pkg != null ? pkg.getImplementationTitle() : "snowflake-jdbc";
  }

  /**
   * Utility method to get the complete jar name with version.
   *
   * @return the jar name with version
   */
  public static String getJdbcJarname() {
    return String.format("%s-%s", getImplementationTitle(), implementVersion);
  }
}
