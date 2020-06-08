package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryArrow;
import org.junit.experimental.categories.Category;

/**
 * Test SnowflakeResultSetSerializable for Arrow
 */
@Category(TestCategoryArrow.class)
public class SnowflakeResultSetSerializableArrowIT extends SnowflakeResultSetSerializableIT
{
  public SnowflakeResultSetSerializableArrowIT()
  {
    super("arrow");
  }

}
