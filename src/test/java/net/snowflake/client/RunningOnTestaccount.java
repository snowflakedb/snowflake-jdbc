package net.snowflake.client;

import net.snowflake.client.jdbc.SnowflakeUtil;

public class RunningOnTestaccount implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return SnowflakeUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT").contains("testaccount");
  }
}
