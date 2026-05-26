package net.snowflake.client.internal.core;

import java.util.Map;
import java.util.Properties;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;

/**
 * Provenance of the connection-identifier fields the user supplied. All fields reflect intent at
 * the moment of input, not the final post-normalization state of the connection — so e.g. an {@code
 * account} synthesized later from a host substring is not counted as {@code accountProvided}.
 *
 * <p>Field semantics (mirrors the {@code client_connection_identifier_shape} wire format shared
 * with the Go, Python, and Node.js drivers):
 *
 * <ul>
 *   <li>{@code accountProvided}: the user explicitly set an account identifier (via the {@code
 *       ?account=} URL query param, the {@code account} {@link Properties} key, or the {@code
 *       account = ...} key in {@code connections.toml}).
 *   <li>{@code accountWithRegion}: the raw account string the user typed contained a dot (e.g.
 *       {@code "myacct.us-east-1"}), signaling the deprecated {@code "account.region"} embedded
 *       form. Set only on the raw input.
 *   <li>{@code accountOrgProvided}: the raw account string carried a dash in its <em>account
 *       portion</em> (the substring before the first dot), signaling the org-prefixed form (e.g.
 *       {@code "myorg-myacct"}). Region-portion dashes such as the {@code -east-} inside {@code
 *       "us-east-1"} are intentionally not counted; otherwise {@code "myacct.us-east-1"} would be
 *       misclassified as org-prefixed.
 *   <li>{@code regionProvided}: the user explicitly set a region as a distinct field. JDBC has no
 *       {@code ?region=} URL param, no {@code region} {@link Properties} key, and no {@code REGION}
 *       session property, so this flag is structurally always {@code false} for the JDBC driver. It
 *       is still emitted (as {@code "false"}) to keep the cross-driver join schema byte-identical.
 *   <li>{@code hostProvided}: the user explicitly set a host. {@link
 *       net.snowflake.client.internal.jdbc.SnowflakeConnectString#parse(String, Properties)}
 *       requires a non-empty host on the JDBC URL form, so this is effectively always {@code true}
 *       for URL+Properties connections; the only place it varies is the {@code jdbc:snowflake:auto}
 *       TOML auto-config path, where the host may be synthesized from {@code account = ...}.
 * </ul>
 *
 * <p>TODO(SNOW-3548350): remove this class and its callers after the connection-identifier-shape
 * data collection wraps up (target: 2026-11-30).
 */
public final class ConnectionIdentifierShape {

  // TODO(SNOW-3548350): wire-format keys for the client_connection_identifier_shape telemetry
  // event. The five keys are byte-identical across all four Snowflake drivers.
  public static final String ACCOUNT_PROVIDED_KEY = "account_provided";

  public static final String ACCOUNT_WITH_REGION_KEY = "account_with_region";
  public static final String ACCOUNT_ORG_PROVIDED_KEY = "account_org_provided";
  public static final String REGION_PROVIDED_KEY = "region_provided";
  public static final String HOST_PROVIDED_KEY = "host_provided";

  private final boolean accountProvided;
  private final boolean accountWithRegion;
  private final boolean accountOrgProvided;
  private final boolean regionProvided;
  private final boolean hostProvided;

  ConnectionIdentifierShape(
      boolean accountProvided,
      boolean accountWithRegion,
      boolean accountOrgProvided,
      boolean regionProvided,
      boolean hostProvided) {
    this.accountProvided = accountProvided;
    this.accountWithRegion = accountWithRegion;
    this.accountOrgProvided = accountOrgProvided;
    this.regionProvided = regionProvided;
    this.hostProvided = hostProvided;
  }

  public boolean isAccountProvided() {
    return accountProvided;
  }

  public boolean isAccountWithRegion() {
    return accountWithRegion;
  }

  public boolean isAccountOrgProvided() {
    return accountOrgProvided;
  }

  public boolean isRegionProvided() {
    return regionProvided;
  }

  public boolean isHostProvided() {
    return hostProvided;
  }

  /**
   * Capture the shape from the raw JDBC URL + {@link Properties} pair that {@link
   * net.snowflake.client.internal.jdbc.SnowflakeConnectString#parse(String, Properties)} would
   * consume. Must be invoked on the user's literal input, before any normalization.
   *
   * <p>JDBC URL form requires a non-empty host, so {@code hostProvided} is always {@code true} on
   * this path. {@code regionProvided} is always {@code false} because JDBC exposes no region knob.
   * Account provenance is examined on the value the user typed for {@code ?account=...} or {@code
   * Properties.account}, taking precedence in the same order {@code SnowflakeConnectString} does
   * (URL query first, then Properties overwrite).
   */
  public static ConnectionIdentifierShape captureFromUrlAndProperties(
      String url, Properties properties) {
    String rawAccount = extractRawAccount(url, properties);
    boolean accountProvided = rawAccount != null && !rawAccount.isEmpty();
    boolean accountWithRegion = false;
    boolean accountOrgProvided = false;
    if (accountProvided) {
      int dot = rawAccount.indexOf('.');
      accountWithRegion = dot > 0;
      String accountPortion = dot > 0 ? rawAccount.substring(0, dot) : rawAccount;
      accountOrgProvided = accountPortion.indexOf('-') >= 0;
    }
    // hostProvided=true because SnowflakeConnectString.parse rejects an empty host with
    // INVALID_CONNECT_STRING; the URL form structurally cannot reach this point without one.
    // regionProvided=false because JDBC has no region knob; see class javadoc.
    return new ConnectionIdentifierShape(
        accountProvided, accountWithRegion, accountOrgProvided, /* regionProvided= */ false, true);
  }

  /**
   * Capture the shape from a connections.toml configuration map. Must be invoked <em>before</em>
   * the host-synthesis step in {@link
   * net.snowflake.client.internal.config.SFConnectionConfigParser#buildConnectionParameters(String,
   * Map, java.util.List)} — otherwise a host synthesized from {@code account = ...} would be
   * indistinguishable from a user-supplied one.
   */
  public static ConnectionIdentifierShape captureFromTomlConfig(Map<String, String> config) {
    if (config == null) {
      return empty();
    }
    String rawAccount = config.get("account");
    boolean accountProvided = rawAccount != null && !rawAccount.isEmpty();
    boolean accountWithRegion = false;
    boolean accountOrgProvided = false;
    if (accountProvided) {
      int dot = rawAccount.indexOf('.');
      accountWithRegion = dot > 0;
      String accountPortion = dot > 0 ? rawAccount.substring(0, dot) : rawAccount;
      accountOrgProvided = accountPortion.indexOf('-') >= 0;
    }
    String rawHost = config.get("host");
    boolean hostProvided = rawHost != null && !rawHost.isEmpty();
    // regionProvided=false unconditionally; JDBC has no region knob in connections.toml either.
    return new ConnectionIdentifierShape(
        accountProvided,
        accountWithRegion,
        accountOrgProvided,
        /* regionProvided= */ false,
        hostProvided);
  }

  /** A shape with every flag {@code false}; useful for defensive defaults. */
  public static ConnectionIdentifierShape empty() {
    return new ConnectionIdentifierShape(false, false, false, false, false);
  }

  /**
   * Pull the raw user-typed account string out of the URL query string and the {@link Properties}
   * bag. Precedence: Properties wins iff non-empty, otherwise the URL value (which itself may be
   * null). Returns {@code null} when neither source supplies a non-empty value.
   *
   * <p>This deviates slightly from {@link
   * net.snowflake.client.internal.jdbc.SnowflakeConnectString#parse(String, Properties)}, which
   * unconditionally lets the Properties value overwrite the URL value (so an explicit {@code
   * Properties.account=""} would clear an URL-supplied account). The divergence is harmless: when
   * Properties contains {@code account=""}, the parser rejects the connection with {@code
   * INVALID_CONNECT_STRING}, so the post-login telemetry never fires.
   */
  private static String extractRawAccount(String url, Properties properties) {
    String fromUrl = accountFromUrlQuery(url);
    String fromProps = null;
    if (properties != null) {
      // Match SnowflakeConnectString.parse case-insensitive key matching for "account".
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        if ("account".equalsIgnoreCase(String.valueOf(entry.getKey()))) {
          Object value = entry.getValue();
          if (value instanceof String) {
            fromProps = (String) value;
            break;
          }
        }
      }
    }
    if (fromProps != null && !fromProps.isEmpty()) {
      return fromProps;
    }
    return fromUrl;
  }

  private static String accountFromUrlQuery(String url) {
    if (SnowflakeUtil.isNullOrEmpty(url)) {
      return null;
    }
    int q = url.indexOf('?');
    if (q < 0) {
      return null;
    }
    String query = url.substring(q + 1);
    for (String pair : query.split("&")) {
      int eq = pair.indexOf('=');
      if (eq <= 0) {
        continue;
      }
      String key = pair.substring(0, eq);
      String value = pair.substring(eq + 1);
      if ("account".equalsIgnoreCase(key) && !value.isEmpty()) {
        return value;
      }
    }
    return null;
  }
}
