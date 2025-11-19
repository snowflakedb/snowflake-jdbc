package net.snowflake.client.internal.jdbc.util;

import net.snowflake.client.api.driver.SnowflakeDriver;

public class DriverUtil {

  public static String getImplementationVersion() {
    return SnowflakeDriver.getImplementationVersion();
  }
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
    return String.format("%s-%s", getImplementationTitle(), getImplementationVersion());
  }
}
