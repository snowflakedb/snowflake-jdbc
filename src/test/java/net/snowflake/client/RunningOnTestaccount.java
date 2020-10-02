package net.snowflake.client;

public class RunningOnTestaccount implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return "testaccount".equals(TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT"));
  }
}
