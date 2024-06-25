package net.snowflake.client;

import net.snowflake.client.core.Constants;

public class RunningNotOnWin implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.WINDOWS;
  }
}
