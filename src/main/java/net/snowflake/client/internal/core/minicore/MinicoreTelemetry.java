package net.snowflake.client.internal.core.minicore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.util.SecretDetector;

/**
 * Telemetry data for minicore library loading and platform information.
 *
 * <p>This class encapsulates all telemetry information related to minicore that should be included
 * in the CLIENT_ENVIRONMENT section of the login-request.
 *
 * <p>Fields:
 *
 * <ul>
 *   <li><b>ISA</b>: Instruction Set Architecture (e.g., "amd64", "arm64")
 *   <li><b>CORE_VERSION</b>: Result of sf_core_full_version() if successful
 *   <li><b>CORE_FILE_NAME</b>: Binary library file name that the driver tried to load
 *   <li><b>CORE_LOAD_ERROR</b>: One of three error states from {@link MinicoreLoadError}
 *   <li><b>loadLogs</b>: List of log messages with detailed error info from the loading process
 * </ul>
 *
 * <p>Note: OS and OS_VERSION are already set by SessionUtil.createClientEnvironmentInfo()
 */
@SnowflakeJdbcInternalApi
public class MinicoreTelemetry {

  private final String isa;
  private final String coreVersion;
  private final String coreFileName;
  private final MinicoreLoadError coreLoadError;
  private final List<String> coreLoadLogs;

  private MinicoreTelemetry(
      String isa,
      String coreVersion,
      String coreFileName,
      MinicoreLoadError coreLoadError,
      List<String> coreLoadLogs) {
    this.isa = isa;
    this.coreVersion = coreVersion;
    this.coreFileName = coreFileName;
    this.coreLoadError = coreLoadError;
    this.coreLoadLogs = coreLoadLogs != null ? coreLoadLogs : new ArrayList<>();
  }

  /**
   * Create telemetry based on current Minicore state.
   *
   * <p>Handles three error cases:
   *
   * <ul>
   *   <li>Disabled via env var: {@link MinicoreLoadError#DISABLED}
   *   <li>Still loading: {@link MinicoreLoadError#STILL_LOADING}
   *   <li>Failed to load: {@link MinicoreLoadError#FAILED_TO_LOAD}
   * </ul>
   */
  public static MinicoreTelemetry create() {
    String isa = Constants.getArchitecture().getIdentifier();

    // Check if disabled via environment variable
    if (Minicore.isDisabledViaEnvVar()) {
      return new MinicoreTelemetry(isa, null, null, MinicoreLoadError.DISABLED, null);
    }

    // Check if initialization hasn't started or instance not yet available
    Minicore minicore = Minicore.getInstance();
    if (minicore == null) {
      return new MinicoreTelemetry(isa, null, null, MinicoreLoadError.STILL_LOADING, null);
    }

    MinicoreLoadResult result = minicore.getLoadResult();
    if (result == null) {
      return new MinicoreTelemetry(isa, null, null, MinicoreLoadError.STILL_LOADING, null);
    }

    return fromLoadResult(result);
  }

  /** Create telemetry from a successful or failed load result. */
  public static MinicoreTelemetry fromLoadResult(MinicoreLoadResult loadResult) {
    String isa = Constants.getArchitecture().getIdentifier();
    String coreVersion = loadResult.getCoreVersion();
    String coreFileName = loadResult.getLibraryFileName();
    List<String> logs = new ArrayList<>(loadResult.getLogs());

    if (loadResult.isSuccess()) {
      return new MinicoreTelemetry(isa, coreVersion, coreFileName, null, logs);
    }

    // For failures, add the detailed error message to logs for visibility
    String detailedError = loadResult.getErrorMessage();
    if (detailedError != null && !detailedError.isEmpty()) {
      logs.add("Error: " + detailedError);
    }
    Throwable exception = loadResult.getException();
    if (exception != null) {
      logs.add("Exception: " + exception.getClass().getName() + ": " + exception.getMessage());
    }

    return new MinicoreTelemetry(
        isa, coreVersion, coreFileName, MinicoreLoadError.FAILED_TO_LOAD, logs);
  }

  // Convert telemetry data to Map for client environment telemetry. Load logs are not included.
  public Map<String, Object> toClientEnvironmentTelemetryMap() {
    Map<String, Object> map = new HashMap<>();

    if (isa != null) {
      map.put("ISA", SecretDetector.maskSecrets(isa));
    }

    if (coreVersion != null) {
      map.put("CORE_VERSION", SecretDetector.maskSecrets(coreVersion));
    }

    if (coreFileName != null) {
      map.put("CORE_FILE_NAME", SecretDetector.maskSecrets(coreFileName));
    }

    if (coreLoadError != null) {
      map.put("CORE_LOAD_ERROR", SecretDetector.maskSecrets(coreLoadError.getMessage()));
    }

    return map;
  }

  // Convert telemetry data to ObjectNode for in-band telemetry.
  public ObjectNode toInBandTelemetryNode() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    ObjectNode message = mapper.createObjectNode();

    message.put("type", "client_minicore_load");
    message.put("source", "JDBC");
    message.put("success", coreLoadError == null);

    if (coreFileName != null) {
      message.put("libraryFileName", SecretDetector.maskSecrets(coreFileName));
    }

    if (coreVersion != null) {
      message.put("coreVersion", SecretDetector.maskSecrets(coreVersion));
    }

    if (coreLoadError != null) {
      message.put("error", SecretDetector.maskSecrets(coreLoadError.getMessage()));
    }

    if (!coreLoadLogs.isEmpty()) {
      ArrayNode logsArray = message.putArray("loadLogs");
      coreLoadLogs.stream().map(SecretDetector::maskSecrets).forEach(logsArray::add);
    }

    return message;
  }

  @Override
  public String toString() {
    return String.format(
        "MinicoreTelemetry{isa='%s', coreVersion='%s', coreFileName='%s', coreLoadError='%s', logs=%d entries}",
        isa,
        coreVersion,
        coreFileName,
        coreLoadError != null ? coreLoadError.getMessage() : null,
        coreLoadLogs.size());
  }
}
