/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 *
 * @author jhuang
 */
public class SnowflakeSQLFeatureNotSupportedException
extends SQLFeatureNotSupportedException
{
  static final SFLogger logger = SFLoggerFactory.getLogger(
                      SnowflakeSQLFeatureNotSupportedException.class);

  public SnowflakeSQLFeatureNotSupportedException()
  {
    super();

    logger.warn(
               "Snowflake exception: SQL Feature not supported by Snowflake. ");

  }

  public SnowflakeSQLFeatureNotSupportedException(String reason)
  {
    super();

    logger.warn(
               "Snowflake exception: SQL Feature not supported by Snowflake. "
               + reason);
  }
}
