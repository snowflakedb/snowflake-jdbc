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
 *   <li><b>CORE_LOAD_ERROR</b>: Error message if loading failed
 *   <li><b>CORE_LOAD_LOGS</b>: List of log messages from the loading process
 * </ul>
 *
 * <p>Note: OS and OS_VERSION are already set by SessionUtil.createClientEnvironmentInfo()
 */
@SnowflakeJdbcInternalApi
public class MinicoreTelemetry {

  private final String isa;
  private final String coreVersion;
  private final String coreFileName;
  private final String coreLoadError;
  private final List<String> coreLoadLogs;

  private MinicoreTelemetry(
      String isa,
      String coreVersion,
      String coreFileName,
      String coreLoadError,
      List<String> coreLoadLogs) {
    this.isa = isa;
    this.coreVersion = coreVersion;
    this.coreFileName = coreFileName;
    this.coreLoadError = coreLoadError;
    this.coreLoadLogs = coreLoadLogs != null ? coreLoadLogs : new ArrayList<>();
  }

  /** Create telemetry from the Minicore instance, handling all null cases. */
  public static MinicoreTelemetry from(Minicore minicore) {
    if (minicore == null) {
      return withError("Minicore not initialized");
    }
    MinicoreLoadResult result = minicore.getLoadResult();
    if (result == null) {
      return withError("Minicore load result unavailable");
    }
    return fromLoadResult(result);
  }

  /** Create telemetry from a successful or failed load result. */
  public static MinicoreTelemetry fromLoadResult(MinicoreLoadResult loadResult) {
    String isa = Constants.getArchitecture().getIdentifier();
    String coreVersion = loadResult.getCoreVersion();
    String coreFileName = loadResult.getLibraryFileName();
    String coreLoadError = loadResult.isSuccess() ? null : loadResult.getErrorMessage();
    List<String> logs = new ArrayList<>(loadResult.getLogs());

    return new MinicoreTelemetry(isa, coreVersion, coreFileName, coreLoadError, logs);
  }

  private static MinicoreTelemetry withError(String error) {
    String isa = Constants.getArchitecture().getIdentifier();
    return new MinicoreTelemetry(isa, null, null, error, null);
  }

  // Convert telemetry data to Map for client environment telemetry. Load logs are not included.
  public Map<String, Object> toClientEnvironmentTelemetryMap() {
    Map<String, Object> map = new HashMap<>();

    if (isa != null) {
      map.put("ISA", isa);
    }

    if (coreVersion != null) {
      map.put("CORE_VERSION", coreVersion);
    }

    if (coreFileName != null) {
      map.put("CORE_FILE_NAME", coreFileName);
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
      message.put("libraryFileName", coreFileName);
    }

    if (coreVersion != null) {
      message.put("coreVersion", coreVersion);
    }

    if (coreLoadError != null) {
      message.put("error", coreLoadError);
    }

    if (!coreLoadLogs.isEmpty()) {
      ArrayNode logsArray = message.putArray("loadLogs");
      coreLoadLogs.forEach(logsArray::add);
    }

    return message;
  }

  @Override
  public String toString() {
    return String.format(
        "MinicoreTelemetry{isa='%s', coreVersion='%s', coreFileName='%s', coreLoadError='%s', logs=%d entries}",
        isa, coreVersion, coreFileName, coreLoadError, coreLoadLogs.size());
  }
}
