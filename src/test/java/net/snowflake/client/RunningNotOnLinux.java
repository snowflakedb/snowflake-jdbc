package net.snowflake.client;

import net.snowflake.client.core.Constants;

public class RunningNotOnLinux implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.LINUX;
  }
}
