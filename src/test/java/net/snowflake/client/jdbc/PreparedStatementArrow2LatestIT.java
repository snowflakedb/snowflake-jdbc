/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.junit.experimental.categories.Category;

/**
 * PreparedStatement integration tests for the latest JDBC driver but not the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to PreparedStatement2IT so that
 * both the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryArrow.class)
public class PreparedStatementArrow2LatestIT extends PreparedStatement2LatestIT {
  PreparedStatementArrow2LatestIT() {
    super("arrow");
  }
  ;
}
