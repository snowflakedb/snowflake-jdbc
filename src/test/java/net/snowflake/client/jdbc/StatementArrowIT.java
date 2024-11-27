package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;

@Tag(TestTags.ARROW)
public class StatementArrowIT extends StatementIT {
  public StatementArrowIT() {
    super();
    queryResultFormat = "arrow";
  }
}
