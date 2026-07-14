package net.snowflake.client.internal.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.api.driver.SnowflakeDriver;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryField;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.common.core.LoginInfoDTO;

/**
 * Emit a single {@code client_connection_identifier_shape} in-band telemetry record per successful
 * login. The payload describes which connection-identifier fields the user supplied (account, host,
 * etc.) — no hostname or account value is ever included.
 *
 * <p>This is gated by both the existing server-side {@code CLIENT_TELEMETRY_ENABLED} parameter (via
 * {@link TelemetryClient#isTelemetryEnabled()} called inside {@link TelemetryClient#addLogToBatch})
 * and a local environment kill switch {@link #DISABLE_ENV_VAR} (case-insensitive {@code "true"}),
 * matching the cross-driver convention.
 *
 * <p>TODO(SNOW-3548350): remove this class together with the {@link ConnectionIdentifierShape}
 * capture and the call site in {@link SFSession} after the data collection wraps up (target:
 * 2026-11-30).
 */
public final class ConnectionIdentifierShapeTelemetry {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(ConnectionIdentifierShapeTelemetry.class);

  /**
   * Environment variable that suppresses the connection-identifier-shape telemetry emission. Only
   * the case-insensitive literal {@code "true"} disables; everything else (including former truthy
   * aliases like {@code "1"} / {@code "yes"} / {@code "false"} / empty) leaves emission enabled.
   * Name and semantics match the cross-driver convention documented in the Snowflake JDBC
   * CHANGELOG.
   */
  public static final String DISABLE_ENV_VAR = "SF_TELEMETRY_DISABLE_CONNECTION_SHAPE";

  /**
   * Source value for the {@code "source"} wire field. Matches the existing JDBC in-band telemetry
   * convention (see {@code MinicoreTelemetry.toInBandTelemetryNode}).
   */
  static final String SOURCE_VALUE = LoginInfoDTO.SF_JDBC_APP_ID;

  private ConnectionIdentifierShapeTelemetry() {
    // utility class
  }

  /**
   * Queue a single in-band telemetry log for the supplied shape. The call honors {@link
   * #DISABLE_ENV_VAR} via the actual {@code System.getenv} lookup; for tests prefer {@link
   * #emit(TelemetryClient, ConnectionIdentifierShape, String)}.
   *
   * @param client telemetry client to queue the log on; no-op if {@code null}.
   * @param shape captured shape; no-op if {@code null}.
   */
  public static void emit(TelemetryClient client, ConnectionIdentifierShape shape) {
    emit(client, shape, SnowflakeUtil.systemGetEnv(DISABLE_ENV_VAR));
  }

  /**
   * Visible-for-testing overload that takes the raw env-var value as a parameter, so unit tests can
   * exercise the kill-switch logic without mutating the JVM environment (Java's {@code
   * System.getenv} map is immutable at runtime).
   */
  public static void emit(
      TelemetryClient client, ConnectionIdentifierShape shape, String disableEnvValue) {
    if (disabledByEnv(disableEnvValue)) {
      logger.debug("connection-identifier-shape telemetry disabled via {}", DISABLE_ENV_VAR);
      return;
    }
    if (client == null) {
      logger.trace("connection-identifier-shape telemetry skipped: no telemetry client");
      return;
    }
    if (shape == null) {
      logger.debug("connection-identifier-shape telemetry skipped: shape not captured");
      return;
    }
    ObjectNode node = toTelemetryNode(shape);
    try {
      // addLogToBatch internally consults isTelemetryEnabled(), which honors the server-side
      // CLIENT_TELEMETRY_ENABLED parameter; no additional gating is needed here.
      client.addLogToBatch(node, System.currentTimeMillis());
      logger.trace("Queued connection-identifier-shape telemetry for sending");
    } catch (Exception e) {
      // Never fail the session due to telemetry.
      logger.trace("Failed to queue connection-identifier-shape telemetry: {}", e.getMessage());
    }
  }

  /**
   * Returns whether {@code disableEnvValue} matches the disable convention. Only case-insensitive
   * {@code "true"} disables — no aliases like {@code "1"} / {@code "yes"} and no whitespace
   * tolerance — matching the {@code strings.EqualFold(..., "true")} behavior in the gosnowflake
   * reference (the cross-driver spec) and the byte-identical Python {@code .lower() == "true"} and
   * Node.js {@code .toLowerCase() === "true"} checks. Intentionally NOT calling {@code .trim()}: a
   * shell accidentally exporting {@code " true "} must leave the emission enabled, exactly like the
   * sibling drivers, so an operator setting the kill switch sees the same behavior everywhere.
   */
  static boolean disabledByEnv(String disableEnvValue) {
    return "true".equalsIgnoreCase(disableEnvValue);
  }

  /** Build the ObjectNode that will be addLogToBatch'd. Package-private for unit tests. */
  static ObjectNode toTelemetryNode(ConnectionIdentifierShape shape) {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    ObjectNode message = mapper.createObjectNode();
    message.put(
        TelemetryField.TYPE.toString(), TelemetryField.CONNECTION_IDENTIFIER_SHAPE.toString());
    message.put(TelemetryField.SOURCE.toString(), SOURCE_VALUE);
    message.put(TelemetryField.DRIVER_TYPE.toString(), LoginInfoDTO.SF_JDBC_APP_ID);
    message.put(
        TelemetryField.DRIVER_VERSION.toString(), SnowflakeDriver.getImplementationVersion());
    message.put(
        ConnectionIdentifierShape.ACCOUNT_PROVIDED_KEY,
        Boolean.toString(shape.isAccountProvided()));
    message.put(
        ConnectionIdentifierShape.ACCOUNT_WITH_REGION_KEY,
        Boolean.toString(shape.isAccountWithRegion()));
    message.put(
        ConnectionIdentifierShape.ACCOUNT_ORG_PROVIDED_KEY,
        Boolean.toString(shape.isAccountOrgProvided()));
    // regionProvided is structurally always false for JDBC (no region knob in URL params,
    // Properties, or connections.toml — see ConnectionIdentifierShape javadoc). The field is
    // still emitted to keep the cross-driver join schema byte-identical.
    message.put(
        ConnectionIdentifierShape.REGION_PROVIDED_KEY, Boolean.toString(shape.isRegionProvided()));
    message.put(
        ConnectionIdentifierShape.HOST_PROVIDED_KEY, Boolean.toString(shape.isHostProvided()));
    return message;
  }
}
