/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.SQLException;

/**
 * Data source unit test
 */
public class SnowflakeBasicDataSourceTest
{
  /**
   * snow-37186
   */
  @Test
  public void testSetLoginTimeout() throws SQLException
  {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();

    ds.setLoginTimeout(10);
    assertThat(ds.getLoginTimeout(), is(10));
  }
}
