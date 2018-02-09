/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Snowflake Loader exception for Test. This should only be valid in tests.
 */
public class SnowflakeSimulatedUploadFailure extends RuntimeException
{

  static final SFLogger logger = SFLoggerFactory.getLogger(
          SnowflakeSimulatedUploadFailure.class);

  public SnowflakeSimulatedUploadFailure()
  {
    super();
    logger.error("This constructor should not be used.");
  }

  public SnowflakeSimulatedUploadFailure(String filename)
  {
    super("Simulated upload failure for " + filename);

    logger.info("{}. This should show up only in tests.", this.getMessage());
  }

}
