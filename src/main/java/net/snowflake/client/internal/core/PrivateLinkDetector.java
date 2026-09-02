package net.snowflake.client.internal.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PrivateLinkDetector {

  private static final String SNOWFLAKE_DOMAIN = ".snowflakecomputing.";

  /**
   * Recognized Snowflake-owned domain suffixes, used when a host is about to be turned into the
   * authority of a URL the driver builds itself. Kept here rather than in any one caller so that
   * every subsystem matches hosts the same way.
   *
   * <p>The apex itself (no subdomain) is accepted, for parity with the other drivers. Extra
   * suffixes (for example {@code SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES}) must not be applied through
   * this list; callers that need an additive hatch match those suffixes themselves via {@link
   * #matchesHostSuffix}.
   */
  private static final List<String> RECOGNIZED_SNOWFLAKE_HOST_SUFFIXES =
      Collections.unmodifiableList(
          Arrays.asList(
              "snowflakecomputing.com", "snowflakecomputing.cn", "snowflakecomputing.mil"));

  /**
   * @param host hostname or URL string
   * @return true if host is a PrivateLink Snowflake environment
   */
  public static boolean isPrivateLink(String host) {
    if (host == null) {
      return false;
    }
    if (!isWellFormedAuthority(extractAuthority(host))) {
      return false;
    }
    String hostname = normalizeHost(extractHostname(host));
    return isLdhHost(hostname)
        && hasRecognizedSnowflakeSuffix(hostname)
        && hostname.contains(".privatelink.");
  }

  /**
   * Extracts the authority of a string that may be a full URL or a bare host: everything after the
   * scheme and before the path. Unlike {@link #extractHostname} this keeps any {@code ":port"} and
   * anything else the authority carries, so {@link #isWellFormedAuthority} can judge it.
   */
  static String extractAuthority(String hostOrUrl) {
    return hostOrUrl.trim().replaceAll("^[^/]*//", "").replaceAll("/.*", "");
  }

  /**
   * Whether an authority is exactly a host, or a host and a decimal port, and nothing else.
   *
   * <p>{@link #extractHostname} discards everything from the first {@code ':'} onward, which is
   * correct for a {@code host:port} authority but means the discarded tail is never examined. A
   * caller that classifies the host portion and then hands the original string to a URL parser
   * would be reasoning about a different authority than the one that gets resolved: in {@code
   * acct.privatelink.snowflakecomputing.com:evil@example.org} the host portion is a Snowflake host
   * while the authority a URL parser resolves is {@code example.org}. Requiring the whole authority
   * to be well formed keeps the classification honest about the entire input.
   */
  static boolean isWellFormedAuthority(String authority) {
    if (authority == null || authority.isEmpty()) {
      return false;
    }
    int colonIndex = authority.indexOf(':');
    if (colonIndex < 0) {
      return isLdhHost(normalizeHost(authority));
    }
    String port = authority.substring(colonIndex + 1);
    if (port.isEmpty() || port.length() > 5) {
      return false;
    }
    int portNumber = 0;
    for (int i = 0; i < port.length(); i++) {
      char c = port.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
      portNumber = portNumber * 10 + (c - '0');
    }
    if (portNumber < 1 || portNumber > 65535) {
      return false;
    }
    return isLdhHost(normalizeHost(authority.substring(0, colonIndex)));
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
    return endsWithSnowflakeDomain(normalizeHost(host));
  }

  /**
   * Normalizes a host (or a configured host suffix) into the single string that every subsequent
   * check and every derived URL must use.
   *
   * <p>Trims, lower-cases ASCII only, drops everything from the first {@code ':'} onward, then
   * strips exactly one trailing {@code '.'} (FQDN form). The port must be dropped before the
   * trailing dot: a host in FQDN form carrying an explicit port ({@code
   * "acct.snowflakecomputing.com.:443"}) still has the dot immediately before the colon, and
   * removing the dot first would leave it attached to the label and match no suffix.
   *
   * <p>Lower-casing is deliberately ASCII-only rather than {@code String.toLowerCase}: the
   * default-locale overload folds {@code 'I'} to a dotless {@code 'ı'} under a Turkish locale, and
   * even {@code Locale.ROOT} folds non-ASCII characters such as U+212A KELVIN SIGN to {@code 'k'}.
   * Either would make this method's view of the host differ from the bytes handed to DNS. Leaving
   * non-ASCII untouched lets {@link #isLdhHost} reject it instead.
   */
  public static String normalizeHost(String host) {
    if (host == null) {
      return "";
    }
    String normalized = toAsciiLowerCase(host.trim());
    int colonIndex = normalized.indexOf(':');
    if (colonIndex >= 0) {
      normalized = normalized.substring(0, colonIndex);
    }
    if (normalized.endsWith(".")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  /**
   * Whether an already-normalized host is made only of characters legal in a DNS hostname.
   *
   * <p>This is an allow-list, not a list of forbidden delimiters, and that distinction is
   * load-bearing. A host is only ever safe to interpolate into a URL authority if it cannot carry a
   * character that some URL parser treats as ending the host. Enumerating such characters does not
   * work: parsers variously terminate the authority at a space, {@code ';'}, {@code '\''}, a
   * percent-escape, or a full-width look-alike of {@code '.'}, {@code '#'} or {@code '?'}. Since a
   * host whose trailing labels are a recognized Snowflake domain can still begin with an unrelated
   * name, a parser that stops early resolves that unrelated name instead.
   *
   * <p>Underscore is accepted: account labels legitimately contain one.
   *
   * @param normalizedHost output of {@link #normalizeHost}
   * @return true if every dot-separated label is non-empty and matches {@code [a-z0-9_-]}
   */
  public static boolean isLdhHost(String normalizedHost) {
    if (normalizedHost == null || normalizedHost.isEmpty()) {
      return false;
    }
    int labelLength = 0;
    for (int i = 0; i < normalizedHost.length(); i++) {
      char c = normalizedHost.charAt(i);
      if (c == '.') {
        if (labelLength == 0) {
          return false; // leading dot, or two dots in a row
        }
        labelLength = 0;
        continue;
      }
      boolean legal = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
      if (!legal) {
        return false;
      }
      labelLength++;
    }
    return labelLength > 0; // reject a trailing empty label
  }

  /**
   * Whether an already-normalized host ends at one of the recognized Snowflake suffixes, matched on
   * a label boundary so that only a listed suffix and its subdomains qualify. Built-in suffixes
   * only; there is no extras parameter so an env hatch cannot widen OCSP or PrivateLink detection.
   *
   * @param normalizedHost output of {@link #normalizeHost}
   */
  public static boolean hasRecognizedSnowflakeSuffix(String normalizedHost) {
    if (normalizedHost == null || normalizedHost.isEmpty()) {
      return false;
    }
    for (String suffix : RECOGNIZED_SNOWFLAKE_HOST_SUFFIXES) {
      if (matchesHostSuffix(normalizedHost, suffix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Label-boundary match of an already-normalized host against a single already-normalized suffix.
   * Callers that accept extra suffixes (WORKLOAD_IDENTITY env hatch) use this in addition to {@link
   * #hasRecognizedSnowflakeSuffix(String)}, rather than passing extras into that method.
   */
  public static boolean matchesHostSuffix(String normalizedHost, String suffix) {
    return normalizedHost != null
        && suffix != null
        && !suffix.isEmpty()
        && (normalizedHost.equals(suffix) || normalizedHost.endsWith("." + suffix));
  }

  private static String toAsciiLowerCase(String value) {
    StringBuilder lowered = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      lowered.append(c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c);
    }
    return lowered.toString();
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
