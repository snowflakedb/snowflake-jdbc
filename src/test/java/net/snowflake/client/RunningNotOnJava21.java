package net.snowflake.client;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

@Deprecated
public class RunningNotOnJava21 implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return isRunningOnJava21();
  }

  public static boolean isRunningOnJava21() {
    return systemGetProperty("java.version").startsWith("21.");
  }
}
