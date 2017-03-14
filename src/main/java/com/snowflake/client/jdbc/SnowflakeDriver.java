/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.client.jdbc;

import java.sql.Driver;

/**
 * This is a hack way of allowing customer to load com.snowflake.client.jdbc.
 * SnowflakeDriver since old customers are using this class. Ideally we want
 * to remove this class and have all customer using net.snowflake.client.
 * jdbc.SnowflakeDriver
 *
 * Created by hyu on 10/10/16.
 */
public class SnowflakeDriver extends net.snowflake.client.jdbc.SnowflakeDriver
      implements Driver
{
}
