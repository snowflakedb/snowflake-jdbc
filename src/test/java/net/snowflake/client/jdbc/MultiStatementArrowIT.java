package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.junit.experimental.categories.Category;

@Category(TestCategoryArrow.class)
public class MultiStatementArrowIT extends MultiStatementIT {

  public MultiStatementArrowIT() {
    super();
    queryResultFormat = "arrow";
  }
}
