package net.snowflake.client.internal.jdbc.cloud.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.common.core.SqlState;

/**
 * Defense-in-depth validator that ensures a resolved local download path stays inside the user's
 * GET target directory.
 *
 * <p>The destination filename used by the file transfer agent and the cloud storage clients is
 * ultimately derived from server-supplied stage object keys. {@link
 * net.snowflake.client.internal.jdbc.SnowflakeFileTransferAgent#extractSafeDestFileName} already
 * sanitizes that name at the source, but new code paths or future refactors could reintroduce an
 * unsanitized basename. Calling {@link #assertWithinDirectory(String, File, String)} immediately
 * before opening the local file for writing prevents any such regression from escaping the target
 * directory via {@code "../"} segments, Windows backslash separators, absolute paths, or symlink
 * traversal.
 *
 * <p>The check uses {@link File#getCanonicalFile()} so it follows symlinks and normalizes any
 * relative components on every supported platform.
 */
public final class DownloadPathValidator {

  private DownloadPathValidator() {}

  /**
   * Verifies that {@code destFile} resolves to a path inside {@code localLocation} after canonical
   * resolution. Throws {@link SnowflakeSQLException} if it does not, or if either path cannot be
   * canonicalized.
   *
   * @param localLocation user-specified local target directory for the GET command
   * @param destFile fully resolved destination file the caller is about to write
   * @param queryId query ID for error reporting (may be {@code null})
   * @throws SnowflakeSQLException if {@code destFile} escapes {@code localLocation} or canonical
   *     resolution fails
   */
  public static void assertWithinDirectory(String localLocation, File destFile, String queryId)
      throws SnowflakeSQLException {
    if (localLocation == null || destFile == null) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Cannot validate download path: localLocation or destFile is null");
    }
    Path baseDir;
    Path resolved;
    try {
      baseDir = new File(localLocation).getCanonicalFile().toPath();
      resolved = destFile.getCanonicalFile().toPath();
    } catch (IOException | InvalidPathException ex) {
      // IOException covers File#getCanonicalFile() failures (e.g. NUL byte rejected by the OS,
      // permission errors). InvalidPathException covers File#toPath() rejecting strings the legacy
      // File API accepts but java.nio.file.Path does not.
      throw new SnowflakeSQLException(
          queryId,
          ex,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Unable to canonicalize download path: " + ex.getMessage());
    }
    if (!resolved.startsWith(baseDir)) {
      throw new SnowflakeSQLException(
          queryId,
          ErrorCode.INTERNAL_ERROR,
          "Resolved download path " + resolved + " is outside the target directory " + baseDir);
    }
  }
}
