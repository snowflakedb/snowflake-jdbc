/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.client.jdbc;

import java.sql.Driver;

/**
 * This is left in to ensure backward compatibility for old customers that are still using the
 * legacy com.snowflake.client.jdbc.SnowflakeDriver. Ideally, we want to remove this class and have
 * all customers move to net.snowflake.client.jdbc.SnowflakeDriver.
 *
 * <p>Created by hyu on 10/10/16.
 */
public class SnowflakeDriver extends net.snowflake.client.jdbc.SnowflakeDriver implements Driver {}
