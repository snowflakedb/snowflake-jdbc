package net.snowflake.client.jdbc;

import java.sql.SQLWarning;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ResourceBundleManager;

/**
 * A simple wrapper extends SQLWarning
 *
 * Created by hyu on 2/21/17.
 */
class SnowflakeSQLWarning extends SQLWarning
{
  static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeSQLException.class);

  static private final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  SnowflakeSQLWarning(String sqlState, int vendorCode, Object...params)
  {
    super(errorResourceBundleManager.getLocalizedMessage(
        String.valueOf(vendorCode), params), sqlState, vendorCode);

    logger.debug("Snowflake warning: {}, sqlState:{}, vendorCode:{}",
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(vendorCode), params),
        sqlState,
        vendorCode);
  }
}
