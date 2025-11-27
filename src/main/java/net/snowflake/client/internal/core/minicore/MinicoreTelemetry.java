package net.snowflake.client.internal.core.minicore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.Constants.Architecture;
import net.snowflake.client.core.Constants.OS;
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
  private final String coreLoadError;
  private final List<String> coreLoadLogs;

  private MinicoreTelemetry(
      String isa,
      String coreVersion,
      String coreLoadError,
      List<String> coreLoadLogs) {
    this.isa = isa;
    this.coreVersion = coreVersion;
    this.coreLoadError = coreLoadError;
    this.coreLoadLogs = coreLoadLogs != null ? coreLoadLogs : new ArrayList<>();
  }

  public static MinicoreTelemetry fromLoadResult(MinicoreLoadResult loadResult, String coreVersion) {
    if (loadResult == null) {
      return createNotInitialized();
    }

    String isa = normalizeArchitecture(Constants.getArchitecture());
    String coreLoadError = loadResult.isSuccess() ? null : loadResult.getErrorMessage();
    List<String> logs = new ArrayList<>(loadResult.getLogs());

    return new MinicoreTelemetry(isa, coreVersion, coreLoadError, logs);
  }

  private static MinicoreTelemetry createNotInitialized() {
    String isa = normalizeArchitecture(Constants.getArchitecture());
    return new MinicoreTelemetry(isa, null, "MinicoreManager not initialized", null);
  }

  private static String normalizeArchitecture(Architecture arch) {
    if (arch == null) {
      return "Unknown";
    }

    switch (arch) {
      case X86_64:
        return "amd64";
      case AARCH64:
        return "arm64";
      case PPC64:
        return "ppc64";
      case X86:
        return "x86";
      default:
        return "Unknown";
    }
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();

    if (isa != null) {
      map.put("ISA", isa);
    }

    if (coreVersion != null) {
      map.put("CORE_VERSION", coreVersion);
    }

    if (coreLoadError != null) {
      map.put("CORE_LOAD_ERROR", coreLoadError);
    }

    if (!coreLoadLogs.isEmpty()) {
      map.put("CORE_LOAD_LOGS", coreLoadLogs);
    }

    return map;
  }

  @Override
  public String toString() {
    return String.format(
        "MinicoreTelemetry{isa='%s', coreVersion='%s', coreLoadError='%s', logs=%d entries}",
        isa, coreVersion, coreLoadError, coreLoadLogs.size());
  }
}
