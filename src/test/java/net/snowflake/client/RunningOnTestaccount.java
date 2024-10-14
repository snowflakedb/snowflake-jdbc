package net.snowflake.client;

@Deprecated
public class RunningOnTestaccount {
  public boolean isSatisfied() {
    return TestUtil.systemGetEnv("SNOWFLAKE_TEST_ACCOUNT").contains("testaccount");
  }
}
