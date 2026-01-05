package net.snowflake.client.internal.core.minicore;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class Minicore {
  private static final SFLogger logger = SFLoggerFactory.getLogger(Minicore.class);

  public static final String DISABLE_MINICORE_ENV_VAR = "SNOWFLAKE_DISABLE_MINICORE";
  public static final String LIBRARY_BASE_NAME = "libsf_mini_core";

  private static volatile Minicore INSTANCE;
  private static volatile CompletableFuture<Void> INITIALIZATION_FUTURE;
  private static boolean DISABLED_VIA_ENV_VAR = false;

  private final MinicoreLoadResult loadResult;
  private final MinicoreLibrary library;

  private Minicore(MinicoreLoadResult loadResult, MinicoreLibrary library) {
    this.loadResult = loadResult;
    this.library = library;
  }

  public static synchronized void initializeAsync() {
    if (INITIALIZATION_FUTURE != null) {
      return; // Already started
    }

    // Check if minicore is disabled via environment variable
    if (isMinicoreDisabled()) {
      logger.debug(
          "Minicore initialization disabled via {} environment variable", DISABLE_MINICORE_ENV_VAR);
      DISABLED_VIA_ENV_VAR = true;
      INITIALIZATION_FUTURE = CompletableFuture.completedFuture(null);
      return;
    }

    INITIALIZATION_FUTURE =
        CompletableFuture.runAsync(
            () -> {
              try {
                logger.trace("Starting async minicore initialization");
                MinicoreLoader loader = new MinicoreLoader();
                MinicoreLoadResult result = loader.loadLibrary();
                INSTANCE = new Minicore(result, result.getLibrary());
              } catch (Exception e) {
                logger.debug("Unexpected error during minicore initialization", e);
                MinicoreLoadResult failedResult =
                    MinicoreLoadResult.failure(
                        "Unexpected initialization error: " + e.getMessage(),
                        null,
                        e,
                        Collections.emptyList());
                INSTANCE = new Minicore(failedResult, null);
              }
            });
  }

  private static boolean isMinicoreDisabled() {
    String envValue = SnowflakeUtil.systemGetEnv(DISABLE_MINICORE_ENV_VAR);
    return envValue != null && envValue.equalsIgnoreCase("true");
  }

  public static synchronized void initialize() {
    if (INSTANCE != null) {
      return;
    }

    if (INITIALIZATION_FUTURE == null) {
      initializeAsync();
    }

    try {
      INITIALIZATION_FUTURE.join();
    } catch (Exception e) {
      logger.error("Failed to initialize minicore", e);
    }
  }

  public static Minicore getInstance() {
    return INSTANCE;
  }

  public MinicoreLibrary getLibrary() {
    return library;
  }

  public MinicoreLoadResult getLoadResult() {
    return loadResult;
  }

  public static boolean isDisabledViaEnvVar() {
    return DISABLED_VIA_ENV_VAR;
  }

  // This method is for testing only. Do not use in production code
  public static synchronized void resetForTesting() {
    INSTANCE = null;
    INITIALIZATION_FUTURE = null;
    DISABLED_VIA_ENV_VAR = false;
  }
}
