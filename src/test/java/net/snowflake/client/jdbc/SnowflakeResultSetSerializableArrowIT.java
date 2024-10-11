package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;

/** Test SnowflakeResultSetSerializable for Arrow */
//@Category(TestCategoryArrow.class)
@Tag(TestTags.ARROW)
public class SnowflakeResultSetSerializableArrowIT extends SnowflakeResultSetSerializableIT {
  public SnowflakeResultSetSerializableArrowIT() {
    super("arrow");
  }
}
