package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;

/**
 * Factory class that presents the implementation of a Snowflake Connection. This allows for
 * alternate definitions of SFSession, SFStatement, and SFResultSet, (representing the 'physical'
 * implementation layer) that can share high-level code.
 */
public interface SnowflakeConnectionImpl {
  SFSession getSFSession();
  SFStatement createSFStatement();
}
