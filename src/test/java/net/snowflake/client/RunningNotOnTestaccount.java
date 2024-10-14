package net.snowflake.client;

import static net.snowflake.client.RunningOnGithubAction.isRunningOnGithubAction;

@Deprecated
public class RunningNotOnTestaccount {
  public boolean isSatisfied() {
    return (!("testaccount".equals(TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT")))
        || isRunningOnGithubAction());
  }
}
