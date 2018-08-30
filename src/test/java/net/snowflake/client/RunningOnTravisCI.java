package net.snowflake.client;

/**
 * Created by hyu on 1/22/18.
 */
public class RunningOnTravisCI implements
    ConditionalIgnoreRule.IgnoreCondition
{
  //TODO Right now always return false. After we open source the tests, this
  //TODO method will be changed to conditional ignore running some tests on travis
  public boolean isSatisfied()
  {
    return System.getenv("TRAVIS_JOB_ID") != null;
  }
}
