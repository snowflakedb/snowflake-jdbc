package net.snowflake.client.core;

import static net.snowflake.client.core.FileUtil.isWritable;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.File;

@SnowflakeJdbcInternalApi
public class FileCacheUtil {
  public static File getDefaultCacheDir() {
    if (Constants.getOS() == Constants.OS.LINUX) {
      String xdgCacheHome = getXdgCacheHome();
      if (xdgCacheHome != null) {
        return new File(xdgCacheHome, "snowflake");
      }
    }

    String homeDir = getHomeDirProperty();
    if (homeDir == null) {
      // if still home directory is null, no cache dir is set.
      return null;
    }
    if (Constants.getOS() == Constants.OS.WINDOWS) {
      return new File(
          new File(new File(new File(homeDir, "AppData"), "Local"), "Snowflake"), "Caches");
    } else if (Constants.getOS() == Constants.OS.MAC) {
      return new File(new File(new File(homeDir, "Library"), "Caches"), "Snowflake");
    } else {
      return new File(new File(homeDir, ".cache"), "snowflake");
    }
  }

  private static String getXdgCacheHome() {
    String xdgCacheHome = systemGetEnv("XDG_CACHE_HOME");
    if (xdgCacheHome != null && isWritable(xdgCacheHome)) {
      return xdgCacheHome;
    }
    return null;
  }

  private static String getHomeDirProperty() {
    String homeDir = systemGetProperty("user.home");
    if (homeDir != null && isWritable(homeDir)) {
      return homeDir;
    }
    return null;
  }
}
