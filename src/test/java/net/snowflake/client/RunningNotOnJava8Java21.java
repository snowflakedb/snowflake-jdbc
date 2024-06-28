package net.snowflake.client;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

public class RunningNotOnJava8Java21 implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return isRunningOnJava8Java21();
  }

  public static boolean isRunningOnJava8Java21() {
    return systemGetProperty("java.version").startsWith("1.8.0")
        || systemGetProperty("java.version").startsWith("21.");
  }
}
