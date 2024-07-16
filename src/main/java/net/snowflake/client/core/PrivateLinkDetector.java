package net.snowflake.client.core;

@SnowflakeJdbcInternalApi
public class PrivateLinkDetector {
  /**
   * We can only tell if private link is enabled for certain hosts when the hostname contains the
   * word 'privatelink' but we don't have a good way of telling if a private link connection is
   * expected for internal stages for example.
   */
  public static boolean isPrivateLink(String host) {
    return host.toLowerCase().contains(".privatelink.snowflakecomputing.");
  }
}
