/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;

/** Test PreparedStatement in ARROW format 2/2 */
// @Category(TestCategoryArrow.class)
@Tag(TestTags.ARROW)
public class PreparedStatementArrow1IT extends PreparedStatement1IT {
  public PreparedStatementArrow1IT() {
    super("arrow");
  }
}
