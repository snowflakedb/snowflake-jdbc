package net.snowflake.client;

public class RunningOnGCPOrTestaccount implements
                                  ConditionalIgnoreRule.IgnoreCondition
{
  public boolean isSatisfied()
  {
    String host = System.getenv("SNOWFLAKE_TEST_HOST");
    return host != null && host.indexOf(".gcp.") > 0 ||
           "testaccount".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"));
  }
}
