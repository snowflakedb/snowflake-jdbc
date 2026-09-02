package net.snowflake.client.internal.core;

public class PrivateLinkDetector {

  private static final String SNOWFLAKE_DOMAIN = ".snowflakecomputing.";

  /**
   * @param host hostname or URL string
   * @return true if host is a PrivateLink Snowflake environment
   */
  public static boolean isPrivateLink(String host) {
    if (host == null) {
      return false;
    }
    String hostname = extractHostname(host).toLowerCase();
    return endsWithSnowflakeDomain(hostname) && hostname.contains(".privatelink.");
  }

  /**
   * Validates that the given host belongs to the Snowflake domain (*.snowflakecomputing.{tld}).
   *
   * @param host bare hostname to validate
   * @return true if host is a valid Snowflake domain
   */
  public static boolean isSnowflakeHost(String host) {
    if (host == null) {
      return false;
    }
    return endsWithSnowflakeDomain(host.toLowerCase());
  }

  /**
   * A valid Snowflake hostname has the form {@code <account-labels>.snowflakecomputing.<tld>} where
   * there is at least one label before the domain and the TLD is a single alphabetic segment.
   */
  private static boolean endsWithSnowflakeDomain(String lower) {
    int idx = lower.lastIndexOf(SNOWFLAKE_DOMAIN);
    if (idx <= 0) {
      return false;
    }
    String tld = lower.substring(idx + SNOWFLAKE_DOMAIN.length());
    return !tld.isEmpty()
        && tld.indexOf('.') < 0
        && tld.chars().allMatch(c -> c >= 'a' && c <= 'z');
  }

  /**
   * Extracts the hostname from a string that may be a full URL or a bare hostname. Returns the host
   * portion without scheme, port, or path.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code "https://host.com:443/path"} → {@code "host.com"}
   *   <li>{@code "host.com"} → {@code "host.com"}
   * </ul>
   *
   * First replace strips everything up to and including "://" (the scheme). Second replace strips
   * everything from the first ":" or "/" onward (port and path).
   */
  static String extractHostname(String hostOrUrl) {
    return hostOrUrl.replaceAll("^[^/]*//", "").replaceAll("[:/].*", "");
  }
}
