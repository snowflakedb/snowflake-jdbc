package net.snowflake.client;

import static net.snowflake.client.RunningOnGithubAction.isRunningOnGithubAction;

import net.snowflake.client.jdbc.SnowflakeUtil;

public class RunningNotOnTestaccount implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return (!("testaccount".equals(SnowflakeUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT")))
        || isRunningOnGithubAction());
  }
}
