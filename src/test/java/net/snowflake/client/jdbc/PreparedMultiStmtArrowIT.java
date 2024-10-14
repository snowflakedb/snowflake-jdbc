package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;

// @Category(TestCategoryArrow.class)
@Tag(TestTags.ARROW)
public class PreparedMultiStmtArrowIT extends PreparedMultiStmtIT {
  public PreparedMultiStmtArrowIT() {
    super();
    queryResultFormat = "arrow";
  }
}
