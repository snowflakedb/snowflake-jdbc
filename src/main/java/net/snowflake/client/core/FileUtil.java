package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collection;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class FileUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(FileUtil.class);
  private static final Collection<PosixFilePermission> WRITE_BY_OTHERS =
      Arrays.asList(PosixFilePermission.GROUP_WRITE, PosixFilePermission.OTHERS_WRITE);
  private static final Collection<PosixFilePermission> READ_BY_OTHERS =
      Arrays.asList(PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ);
  private static final Collection<PosixFilePermission> EXECUTABLE =
      Arrays.asList(
          PosixFilePermission.OWNER_EXECUTE,
          PosixFilePermission.GROUP_EXECUTE,
          PosixFilePermission.OTHERS_EXECUTE);

  public static void logFileUsage(Path filePath, String context, boolean logReadAccess) {
    logWarnWhenAccessibleByOthers(filePath, context, logReadAccess);
  }

  public static void logFileUsage(File file, String context, boolean logReadAccess) {
    logFileUsage(file.toPath(), context, logReadAccess);
  }

  public static void logFileUsage(String stringPath, String context, boolean logReadAccess) {
    Path path = Paths.get(stringPath);
    logFileUsage(path, context, logReadAccess);
  }

  public static boolean isWritable(String path) {
    File file = new File(path);
    if (!file.canWrite()) {
      logger.debug("File/directory not writeable: {}", path);
      return false;
    }
    return true;
  }

  public static void handleWhenParentDirectoryPermissionsWiderThanUserOnly(
      File file, String context) {
    handleWhenDirectoryPermissionsWiderThanUserOnly(file.getParentFile(), context);
  }

  public static void handleWhenFilePermissionsWiderThanUserOnly(File file, String context) {
    if (Files.isSymbolicLink(file.toPath())) {
      throw new SecurityException("Symbolic link is not allowed for file cache: " + file);
    }
    handleWhenPermissionsWiderThanUserOnly(file.toPath(), context, false);
  }

  public static void handleWhenDirectoryPermissionsWiderThanUserOnly(File file, String context) {
    handleWhenPermissionsWiderThanUserOnly(file.toPath(), context, true);
  }

  public static void handleWhenPermissionsWiderThanUserOnly(
      Path filePath, String context, boolean isDirectory) {
    // we do not check the permissions for Windows
    if (isWindows()) {
      return;
    }

    try {
      Collection<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(filePath);
      boolean isWritableByOthers = isPermPresent(filePermissions, WRITE_BY_OTHERS);
      boolean isReadableByOthers = isPermPresent(filePermissions, READ_BY_OTHERS);
      boolean isExecutable = isPermPresent(filePermissions, EXECUTABLE);

      boolean permissionsTooOpen;
      if (isDirectory) {
        permissionsTooOpen = isWritableByOthers || isReadableByOthers;
      } else {
        permissionsTooOpen = isWritableByOthers || isReadableByOthers || isExecutable;
      }
      if (permissionsTooOpen) {
        logger.debug(
            "{}File/directory {} access rights: {}",
            getContextStr(context),
            filePath,
            filePermissions);
        String message =
            String.format(
                "Access to file or directory %s is wider than allowed. Remove cache file/directory and re-run the driver.",
                filePath);
        if (isDirectory) {
          logger.warn(message);
        } else {
          throw new SecurityException(message);
        }
      } else {
        if (!isDirectory && Files.isSymbolicLink(filePath)) {
          throw new SecurityException("Symbolic link is not allowed for file cache: " + filePath);
        }
      }
    } catch (IOException e) {
      String message =
          String.format(
              "%s Unable to access the file/directory to check the permissions. Error: %s",
              filePath, e);
      if (isDirectory) {
        logger.warn(message);
      } else {
        throw new SecurityException(message);
      }
    }
  }

  private static void logWarnWhenAccessibleByOthers(
      Path filePath, String context, boolean logReadAccess) {
    // we do not check the permissions for Windows
    if (isWindows()) {
      return;
    }

    try {
      Collection<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(filePath);
      logger.debug(
          "{}File {} access rights: {}", getContextStr(context), filePath, filePermissions);

      boolean isWritableByOthers = isPermPresent(filePermissions, WRITE_BY_OTHERS);
      boolean isReadableByOthers = isPermPresent(filePermissions, READ_BY_OTHERS);
      boolean isExecutable = isPermPresent(filePermissions, EXECUTABLE);

      if (isWritableByOthers || (isReadableByOthers && logReadAccess) || isExecutable) {
        logger.warn(
            "{}File {} is accessible by others to:{}{}",
            getContextStr(context),
            filePath,
            isReadableByOthers && logReadAccess ? " read" : "",
            isWritableByOthers ? " write" : "",
            isExecutable ? " execute" : "");
      }
    } catch (IOException e) {
      logger.warn(
          "{}Unable to access the file to check the permissions: {}. Error: {}",
          getContextStr(context),
          filePath,
          e);
    }
  }

  public static void throwWhenOwnerDifferentThanCurrentUser(File file, String context) {
    // we do not check the permissions for Windows
    if (isWindows()) {
      return;
    }

    Path filePath = Paths.get(file.getPath());

    try {
      String fileOwnerName = getFileOwnerName(filePath);
      String currentUser = System.getProperty("user.name");
      if (!currentUser.equalsIgnoreCase(fileOwnerName)) {
        logger.debug(
            "The file owner: {} is different than current user: {}", fileOwnerName, currentUser);
        throw new SecurityException("The file owner is different than current user");
      }
    } catch (IOException e) {
      logger.warn(
          "{}Unable to access the file to check the owner: {}. Error: {}",
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

  static String getFileOwnerName(Path filePath) throws IOException {
    FileOwnerAttributeView ownerAttributeView =
        Files.getFileAttributeView(filePath, FileOwnerAttributeView.class);
    return ownerAttributeView.getOwner().getName();
  }

  private static String getContextStr(String context) {
    return isNullOrEmpty(context) ? "" : context + ": ";
  }

  public static boolean exists(File file) {
    if (file == null) {
      return false;
    }
    return file.exists();
  }
}
