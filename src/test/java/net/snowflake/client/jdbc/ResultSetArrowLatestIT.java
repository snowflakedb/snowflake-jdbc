/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Drop this file when ResultSetLatestIT is dropped.
 */
@Category(TestCategoryArrow.class)
public class ResultSetArrowLatestIT extends ResultSetLatestIT {
  public ResultSetArrowLatestIT() {
    super("arrow");
  }
}
