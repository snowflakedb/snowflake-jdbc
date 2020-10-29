package net.snowflake.client;

public class RunningOnTestaccount implements ConditionalIgnoreRule.IgnoreCondition
{
  public boolean isSatisfied()
  {
    return "testaccount".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"));
  }
}
