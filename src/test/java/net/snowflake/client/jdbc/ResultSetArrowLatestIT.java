/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Drop this file when ResultSetLatestIT is dropped.
 */
// @Category(TestCategoryArrow.class)
@Tag(TestTags.ARROW)
public class ResultSetArrowLatestIT extends ResultSetLatestIT {
  public ResultSetArrowLatestIT() {}
}
