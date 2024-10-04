package net.snowflake.client;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

@Deprecated
public class RunningNotOnJava8 implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return isRunningOnJava8();
  }

  public static boolean isRunningOnJava8() {
    return systemGetProperty("java.version").startsWith("1.8.0");
  }
}
