/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

/** Skip tests on CI when thin jar is tested */
public class SkipOnThinJar implements ConditionalIgnoreRule.IgnoreCondition {
  @Override
  public boolean isSatisfied() {
    return "-Dthin-jar".equals(TestUtil.systemGetEnv("ADDITIONAL_MAVEN_PROFILE"));
  }
}
