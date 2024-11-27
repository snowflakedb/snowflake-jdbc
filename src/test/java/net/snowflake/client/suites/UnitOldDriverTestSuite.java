/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.suites;

import net.snowflake.client.category.TestTags;
import org.junit.platform.suite.api.ExcludeTags;

@ExcludeTags({
  TestTags.CORE,
  TestTags.ARROW,
  TestTags.DIAGNOSTIC,
  TestTags.CONNECTION,
  TestTags.LOADER,
  TestTags.OTHERS,
  TestTags.RESULT_SET,
  TestTags.STATEMENT
})
public class UnitOldDriverTestSuite extends OldDriverTestSuite {}
