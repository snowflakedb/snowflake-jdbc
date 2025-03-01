package net.snowflake.client.suites;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.DisplayName;
import org.junit.platform.suite.api.ExcludeTags;

@BaseTestSuite
@DisplayName("Unit tests")
@ExcludeTags({
  TestTags.CORE,
  TestTags.ARROW,
  TestTags.DIAGNOSTIC,
  TestTags.CONNECTION,
  TestTags.LOADER,
  TestTags.OTHERS,
  TestTags.RESULT_SET,
  TestTags.STATEMENT,
  TestTags.AUTHENTICATION
})
public class UnitTestSuite {}
