package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;

import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

class SystemUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SystemUtil.class);

  /**
   * Helper function to convert system properties to integers
   *
   * @param systemProperty name of the system property
   * @param defaultValue default value used
   * @return the value of the system property, else the default value
   */
  static int convertSystemPropertyToIntValue(String systemProperty, int defaultValue) {
    String systemPropertyValue = systemGetProperty(systemProperty);
    int returnVal = defaultValue;
    if (systemPropertyValue != null) {
      try {
        returnVal = Integer.parseInt(systemPropertyValue);
      } catch (NumberFormatException ex) {
        logger.warn(
            "Failed to parse the system parameter {} with value {}",
            systemProperty,
            systemPropertyValue);
      }
    }
    return returnVal;
  }
}
