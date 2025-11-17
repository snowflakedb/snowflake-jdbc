package net.snowflake.client.api.loader;

import java.sql.Connection;
import java.util.Map;
import net.snowflake.client.internal.loader.StreamLoader;

public class LoaderFactory {

  public static Loader createLoader(
      Map<LoaderProperty, Object> properties,
      Connection uploadConnection,
      Connection processingConnection) {
    return new StreamLoader(properties, uploadConnection, processingConnection);
  }
}
