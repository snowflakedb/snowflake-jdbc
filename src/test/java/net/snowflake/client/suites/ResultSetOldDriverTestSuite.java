/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.suites;

import net.snowflake.client.category.TestTags;
import org.junit.platform.suite.api.IncludeTags;

@IncludeTags(TestTags.RESULT_SET)
public class ResultSetOldDriverTestSuite extends OldDriverTestSuite {}