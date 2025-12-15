package com.snowflake.client.jdbc;

import java.sql.Driver;

/**
 * This is left in to ensure backward compatibility for old customers that are still using the
 * legacy com.snowflake.client.jdbc.SnowflakeDriver. Ideally, we want to remove this class and have
 * all customers move to net.snowflake.client.api.driver.SnowflakeDriver.
 *
 * @deprecated Use {@link net.snowflake.client.api.driver.SnowflakeDriver} instead
 */
@Deprecated
public class SnowflakeDriver extends net.snowflake.client.api.driver.SnowflakeDriver
    implements Driver {}
