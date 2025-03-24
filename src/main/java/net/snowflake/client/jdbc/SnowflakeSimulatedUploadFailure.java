package net.snowflake.client.jdbc;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Snowflake Loader exception for Test. This should only be valid in tests. */
public class SnowflakeSimulatedUploadFailure extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeSimulatedUploadFailure.class);

  public SnowflakeSimulatedUploadFailure() {
    super();
    logger.error("This constructor should not be used.", false);
  }

  public SnowflakeSimulatedUploadFailure(String filename) {
    super("Simulated upload failure for " + filename);

    logger.debug("{}. This should show up only in tests.", this.getMessage());
  }
}
