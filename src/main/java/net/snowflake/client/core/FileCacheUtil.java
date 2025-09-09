package net.snowflake.client.core;

import static net.snowflake.client.core.FileUtil.isWritable;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.File;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class FileCacheUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(FileCacheUtil.class);

  public static File getDefaultCacheDir() {
    if (Constants.getOS() == Constants.OS.LINUX) {
      String xdgCacheHome = getDir(systemGetEnv("XDG_CACHE_HOME"));
      if (xdgCacheHome != null) {
        return new File(xdgCacheHome, "snowflake");
      }
      logger.debug("XDG cache home directory is not set or not writable.");
    }

    String homeDir = getDir(systemGetProperty("user.home"));
    if (homeDir == null) {
      logger.debug("Home directory is not set or not writable, no cache dir is set.");
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

  private static String getDir(String dir) {
    if (dir != null && isWritable(dir)) {
      return dir;
    }
    return null;
  }
}
