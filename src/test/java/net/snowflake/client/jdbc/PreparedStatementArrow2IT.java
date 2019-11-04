/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.junit.experimental.categories.Category;

/**
 * Test PreparedStatement in ARROW format 2/2
 */
@Category(TestCategoryArrow.class)
public class PreparedStatementArrow2IT extends PreparedStatement2IT
{
  public PreparedStatementArrow2IT()
  {
    super("arrow");
  }
}
