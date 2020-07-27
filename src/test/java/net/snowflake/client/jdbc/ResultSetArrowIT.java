package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.junit.experimental.categories.Category;

@Category(TestCategoryArrow.class)
public class ResultSetArrowIT extends ResultSetIT {
  public ResultSetArrowIT() {
    super();
    queryResultFormat = "arrow";
  }
}
