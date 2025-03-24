package net.snowflake.client.loader;

import java.sql.Connection;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class LoaderFactory {
  private static final SFLogger logger = SFLoggerFactory.getLogger(LoaderFactory.class);

  public static Loader createLoader(
      Map<LoaderProperty, Object> properties,
      Connection uploadConnection,
      Connection processingConnection) {
    logger.debug("", false);
    StreamLoader loader = new StreamLoader(properties, uploadConnection, processingConnection);
    return loader;
  }
}
