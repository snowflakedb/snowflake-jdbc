package net.snowflake.client.internal.driver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides access to build-time properties from {@code version.properties}, which is populated by
 * Maven resource filtering.
 */
public final class DriverVersionProperties {

  private static final String RESOURCE = "/net/snowflake/client/jdbc/version.properties";
  private static final Properties PROPERTIES = loadProperties();

  private DriverVersionProperties() {}

  public static String get(String key) {
    return PROPERTIES.getProperty(key);
  }

  private static Properties loadProperties() {
    Properties props = new Properties();
    try (InputStream is = DriverVersionProperties.class.getResourceAsStream(RESOURCE)) {
      if (is != null) {
        props.load(is);
      }
    } catch (IOException e) {
      // Fall through with empty properties — callers will get null and fail with clear messages
    }
    return props;
  }
}
