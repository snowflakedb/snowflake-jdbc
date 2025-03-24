package net.snowflake.client.suites;

import net.snowflake.client.category.TestTags;
import org.junit.platform.suite.api.IncludeTags;

@IncludeTags(TestTags.DIAGNOSTIC)
public class DiagnosticOldDriverTestSuite extends OldDriverTestSuite {}
