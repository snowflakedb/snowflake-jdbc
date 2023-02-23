/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryStatement;
import org.junit.experimental.categories.Category;

/**
 * PreparedStatement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Drop this file when PrepareStatement1IT is dropped.
 */
@Category(TestCategoryStatement.class)
public class PreparedStatementArrow1LatestIT extends PreparedStatement1LatestIT {
  public PreparedStatementArrow1LatestIT() {
    super("arrow");
  }
}
