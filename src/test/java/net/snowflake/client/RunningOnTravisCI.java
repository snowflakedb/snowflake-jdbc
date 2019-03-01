/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

/**
 * Run tests on CI
 */
public class RunningOnTravisCI implements
                               ConditionalIgnoreRule.IgnoreCondition
{
  public boolean isSatisfied()
  {
    return
        System.getenv("TRAVIS_JOB_ID") != null ||
        System.getenv("APPVEYOR_BUILD_ID") != null;
  }
}
