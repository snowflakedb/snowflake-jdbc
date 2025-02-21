package net.snowflake.client;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import net.snowflake.client.core.Constants;

public class AssumptionUtils {
  public static void assumeNotRunningOnGithubActionsMac() {
    assumeFalse(isRunningOnGithubActions() && Constants.getOS() == Constants.OS.MAC);
  }

  public static void assumeNotRunningOnJava8() {
    assumeFalse(systemGetProperty("java.version").startsWith("1.8.0"));
  }

  public static void assumeNotRunningOnJava21() {
    assumeFalse(systemGetProperty("java.version").startsWith("21."));
  }

  public static void assumeRunningOnGithubActions() {
    assumeTrue(isRunningOnGithubActions());
  }

  public static boolean isRunningOnGithubActions() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null;
  }

  public static void assumeRunningOnLinuxMac() {
    assumeTrue(Constants.getOS() == Constants.OS.LINUX || Constants.getOS() == Constants.OS.MAC);
  }
}
