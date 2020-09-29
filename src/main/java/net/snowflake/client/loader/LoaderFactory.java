/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;

import java.sql.Connection;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class LoaderFactory {
  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(LoaderFactory.class);

  public static Loader createLoader(
      Map<LoaderProperty, Object> properties,
      Connection uploadConnection,
      Connection processingConnection) {
    LOGGER.debug("");
    StreamLoader loader = new StreamLoader(properties, uploadConnection, processingConnection);
    return loader;
  }
}
