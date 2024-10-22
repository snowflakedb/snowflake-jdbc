package net.snowflake.client;

import net.snowflake.client.core.Constants;

public class RunningOnWin implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return Constants.getOS() == Constants.OS.WINDOWS;
  }
}
