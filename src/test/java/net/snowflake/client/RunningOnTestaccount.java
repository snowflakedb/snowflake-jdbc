package net.snowflake.client;

@Deprecated
public class RunningOnTestaccount implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT").contains("testaccount");
  }
}
