package net.snowflake.client.internal.core.minicore;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class Minicore {

  private static final SFLogger logger = SFLoggerFactory.getLogger(Minicore.class);

  private static volatile Minicore INSTANCE;

  private final MinicoreLoadResult loadResult;
  private final MinicoreLibrary library;

  private Minicore(MinicoreLoadResult loadResult, MinicoreLibrary library) {
    this.loadResult = loadResult;
    this.library = library;
  }

  public static synchronized void initialize() {
    if (INSTANCE != null) {
      return;
    }

    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();

    INSTANCE = new Minicore(result, loader.getLibrary());

    if (result.isSuccess()) {
      logger.trace("Minicore initialized successfully: {}", result);
    } else {
      logger.trace("Minicore initialized with failed load: {}", result);
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
}
