package net.snowflake.client;

import static net.snowflake.client.RunningOnTravisCI.isRunningOnTravisCI;

public class RunningNotOnTestaccount implements
                                     ConditionalIgnoreRule.IgnoreCondition
{
  public boolean isSatisfied()
  {
    return (!("testaccount".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) || isRunningOnTravisCI());
  }
}
