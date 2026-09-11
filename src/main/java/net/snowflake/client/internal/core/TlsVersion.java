package net.snowflake.client.internal.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * TLS protocol versions the driver can negotiate for the Snowflake API connection.
 *
 * <p>Declaration order is significant: {@link #compareTo} is used to order versions, so constants
 * must stay sorted from oldest to newest.
 */
public enum TlsVersion {
  TLS_1_2("TLSv1.2"),
  TLS_1_3("TLSv1.3");

  public static final TlsVersion DEFAULT_MIN = TLS_1_2;
  public static final TlsVersion DEFAULT_MAX = TLS_1_3;

  private static final SFLogger logger = SFLoggerFactory.getLogger(TlsVersion.class);

  private final String protocolName;

  TlsVersion(String protocolName) {
    this.protocolName = protocolName;
  }

  public String getProtocolName() {
    return protocolName;
  }

  /**
   * @param text protocol name, e.g. {@code TLSv1.3}
   * @return the matching version
   * @throws IllegalArgumentException if {@code text} is null or names no known version
   */
  public static TlsVersion fromString(String text) {
    if (text == null) {
      throw new IllegalArgumentException("TLS version must not be null");
    }
    String trimmed = text.trim();
    for (TlsVersion version : values()) {
      if (version.protocolName.equalsIgnoreCase(trimmed)) {
        return version;
      }
    }
    throw new IllegalArgumentException("Unsupported TLS version: " + text);
  }

  /**
   * Versions named by a JSSE protocol list such as {@code jdk.tls.client.protocols}.
   *
   * <p>Names this driver does not model are ignored rather than rejected: the property is JVM-wide
   * and may legitimately list protocols the driver never offers. Because only TLS 1.2 and TLS 1.3
   * are modelled, an older version named there cannot end up being offered -- the driver's floor is
   * preserved by construction.
   *
   * @return the recognized versions, empty if none were recognized
   */
  static EnumSet<TlsVersion> parseProtocolList(String list) {
    EnumSet<TlsVersion> parsed = EnumSet.noneOf(TlsVersion.class);
    if (list == null) {
      return parsed;
    }
    for (String token : list.split(",")) {
      String trimmed = token.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      for (TlsVersion version : values()) {
        if (version.protocolName.equalsIgnoreCase(trimmed)) {
          parsed.add(version);
          break;
        }
      }
    }
    return parsed;
  }

  /** Protocol names accepted by {@link #fromString}, for error messages. */
  public static String supportedValues() {
    return Arrays.stream(values())
        .map(TlsVersion::getProtocolName)
        .collect(Collectors.joining(", "));
  }

  /**
   * Protocol names within {@code [min, max]} that the JSSE default provider actually supports.
   *
   * <p>Availability is resolved from the provider's supported parameters rather than by probing
   * {@code SSLContext.getInstance(name)}: that probe succeeds even for a protocol removed through
   * the {@code jdk.tls.disabledAlgorithms} security property, which would then be rejected by
   * {@code SSLSocket.setEnabledProtocols} at connection time.
   *
   * @throws IllegalArgumentException if {@code min} is newer than {@code max}
   * @throws IllegalStateException if no version in the range is supported
   */
  static String[] protocolsInRange(TlsVersion min, TlsVersion max) {
    return protocolsInRange(min, max, enabledProtocols());
  }

  /**
   * @param allowed protocol names that may be offered, or null to skip that filter (package-private
   *     so a test can simulate a JVM where a version is banned)
   */
  static String[] protocolsInRange(TlsVersion min, TlsVersion max, Set<String> allowed) {
    if (min.compareTo(max) > 0) {
      throw new IllegalArgumentException(
          String.format(
              "Minimum TLS version %s cannot be greater than the maximum TLS version %s",
              min.getProtocolName(), max.getProtocolName()));
    }

    List<String> selected = new ArrayList<>();
    for (TlsVersion version : values()) {
      if (version.compareTo(min) < 0 || version.compareTo(max) > 0) {
        continue;
      }
      if (allowed != null && !allowed.contains(version.getProtocolName())) {
        logger.debug(
            "TLS protocol {} cannot be enabled on this JVM and will not be offered",
            version.getProtocolName());
        continue;
      }
      selected.add(version.getProtocolName());
    }

    if (selected.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "No TLS version that this JVM can enable matches constraints: min=%s, max=%s",
              min.getProtocolName(), max.getProtocolName()));
    }
    return selected.toArray(new String[0]);
  }

  /**
   * Protocol names the default JSSE provider will actually enable.
   *
   * <p>Deliberately the default-enabled set rather than the supported set. A protocol banned
   * through the {@code jdk.tls.disabledAlgorithms} security property still appears as supported --
   * and {@code setEnabledProtocols} even accepts it -- but the JVM will never negotiate it, so
   * offering it turns a configuration error into an opaque handshake failure.
   *
   * @return names that may be offered, or null when that cannot be determined (callers then skip
   *     the filter rather than offering nothing)
   */
  private static Set<String> enabledProtocols() {
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, null, null);
      return new LinkedHashSet<>(Arrays.asList(context.getDefaultSSLParameters().getProtocols()));
    } catch (Exception ex) {
      logger.debug("Could not determine the protocols this JVM can enable: {}", ex.getMessage());
      return null;
    }
  }
}
