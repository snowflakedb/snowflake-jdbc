package net.snowflake.client.core;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collection;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class FileUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(FileUtil.class);

  static final Collection<PosixFilePermission> WRITE_BY_OTHERS =
      Arrays.asList(PosixFilePermission.GROUP_WRITE, PosixFilePermission.OTHERS_WRITE);
  static final Collection<PosixFilePermission> READ_BY_OTHERS =
      Arrays.asList(PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ);

  public static void logFileUsage(Path filePath, String context, boolean logReadAccess) {
    logger.info("{}Accessing file: {}", getContextStr(context), filePath);
    logWarnWhenAccessibleByOthers(filePath, context, logReadAccess);
  }

  public static void logFileUsage(File file, String context, boolean logReadAccess) {
    logFileUsage(file.toPath(), context, logReadAccess);
  }

  public static void logFileUsage(String stringPath, String context, boolean logReadAccess) {
    Path path = Paths.get(stringPath);
    logFileUsage(path, context, logReadAccess);
  }

  private static void logWarnWhenAccessibleByOthers(
      Path filePath, String context, boolean logReadAccess) {
    // we do not check the permissions for Windows
    if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
      return;
    }

    try {
      Collection<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(filePath);
      logger.debug(
          "{}File {} access rights: {}", getContextStr(context), filePath, filePermissions);

      boolean isWritableByOthers = isPermPresent(filePermissions, WRITE_BY_OTHERS);
      boolean isReadableByOthers = isPermPresent(filePermissions, READ_BY_OTHERS);

      if (isWritableByOthers || (isReadableByOthers && logReadAccess)) {
        logger.warn(
            "{}File {} is accessible by others to:{}{}",
            getContextStr(context),
            filePath,
            isReadableByOthers && logReadAccess ? " read" : "",
            isWritableByOthers ? " write" : "");
      }
    } catch (IOException e) {
      logger.warn(
          "{}Unable to access the file to check the permissions: {}. Error: {}",
          getContextStr(context),
          filePath,
          e);
    }
  }

  private static boolean isPermPresent(
      Collection<PosixFilePermission> filePerms, Collection<PosixFilePermission> permsToCheck)
      throws IOException {
    return filePerms.stream().anyMatch(permsToCheck::contains);
  }

  private static String getContextStr(String context) {
    return Strings.isNullOrEmpty(context) ? "" : context + ": ";
  }
}
