package net.snowflake.client.internal.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.internal.core.Constants;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import org.apache.commons.io.IOUtils;

/**
 * Detects the libc family (glibc / musl) and version on Linux for telemetry and minicore platform
 * targeting purposes.
 */
public final class LibcDetails {

  private static final SFLogger logger = SFLoggerFactory.getLogger(LibcDetails.class);

  public static final String GLIBC = "glibc";
  public static final String MUSL = "musl";

  static final String DEFAULT_LDD_PATH = "/usr/bin/ldd";

  // e.g. "GNU C Library (Ubuntu GLIBC 2.31-0ubuntu9.16) stable release version 2.31."
  private static final Pattern RE_GLIBC_VERSION =
      Pattern.compile("LIBC[-a-z0-9 ).]*?(\\d+\\.\\d+)", Pattern.CASE_INSENSITIVE);
  // e.g. "Version 1.2.3"
  private static final Pattern RE_MUSL_VERSION =
      Pattern.compile("Version\\s+(\\d+\\.\\d+[\\d.]*)", Pattern.CASE_INSENSITIVE);

  // Word-boundary family markers - guard against false positives like "muslib" or "muscle".
  private static final Pattern RE_MUSL_MARKER = Pattern.compile("\\bmusl\\b");
  private static final Pattern RE_GLIBC_NAME_MARKER = Pattern.compile("\\bGNU C Library\\b");
  private static final Pattern RE_GLIBC_GETCONF_MARKER = Pattern.compile("\\bglibc\\b");

  private static final long EXEC_TIMEOUT_MS = 200;

  private static LibcInfo cachedResult;

  private LibcDetails() {}

  /** Returns the cached libc details, performing detection on first call. */
  public static synchronized LibcInfo load() {
    if (cachedResult == null) {
      cachedResult = detect();
    }
    return cachedResult;
  }

  private static LibcInfo detect() {
    if (Constants.getOS() != Constants.OS.LINUX) {
      logger.trace("Libc detection skipped: not running on Linux");
      return new LibcInfo(null, null);
    }

    LibcInfo fromFs = detectFromFilesystem(Paths.get(DEFAULT_LDD_PATH));
    if (fromFs != null && fromFs.getFamily() != null && fromFs.getVersion() != null) {
      return fromFs;
    }

    LibcInfo fromCmd = detectFromCommand();
    if (fromCmd == null) {
      return fromFs != null ? fromFs : new LibcInfo(null, null);
    }
    if (fromFs == null || fromFs.getFamily() == null) {
      return fromCmd;
    }
    // Family already detected; only adopt the command-derived version if both strategies agree
    // on the family. Mixing version from a different family would yield a corrupt result.
    if (fromFs.getVersion() == null && fromFs.getFamily().equals(fromCmd.getFamily())) {
      return new LibcInfo(fromFs.getFamily(), fromCmd.getVersion());
    }
    return fromFs;
  }

  static LibcInfo detectFromFilesystem(Path lddPath) {
    try {
      byte[] bytes = Files.readAllBytes(lddPath);
      String content = new String(bytes, StandardCharsets.UTF_8);
      return parseLddContent(content);
    } catch (IOException e) {
      logger.debug("Failed to read libc details from {}: {}", lddPath, e.getMessage());
      return null;
    } catch (Exception e) {
      logger.debug("Unexpected error reading libc details from {}: {}", lddPath, e.getMessage());
      return null;
    }
  }

  static LibcInfo parseLddContent(String content) {
    if (content == null || content.isEmpty()) {
      return null;
    }

    String family;
    Pattern versionRe;
    if (RE_MUSL_MARKER.matcher(content).find()) {
      family = MUSL;
      versionRe = RE_MUSL_VERSION;
    } else if (RE_GLIBC_NAME_MARKER.matcher(content).find()) {
      family = GLIBC;
      versionRe = RE_GLIBC_VERSION;
    } else {
      return null;
    }

    Matcher m = versionRe.matcher(content);
    String version = m.find() ? m.group(1) : null;
    return new LibcInfo(family, version);
  }

  static LibcInfo detectFromCommand() {
    String output = runLibcVersionCommands();
    if (output == null) {
      return null;
    }
    return parseCommandOutput(output);
  }

  static LibcInfo parseCommandOutput(String output) {
    if (output == null || output.isEmpty()) {
      return null;
    }

    String[] lines = output.split("\\R+");
    String getconfLine = lines.length > 0 ? lines[0] : null;
    String lddLine1 = lines.length > 1 ? lines[1] : null;
    String lddLine2 = lines.length > 2 ? lines[2] : null;

    if (getconfLine != null && RE_GLIBC_GETCONF_MARKER.matcher(getconfLine).find()) {
      String[] parts = getconfLine.trim().split("\\s+");
      String version = parts.length > 1 ? parts[1] : null;
      return new LibcInfo(GLIBC, version);
    }
    if (lddLine1 != null && RE_MUSL_MARKER.matcher(lddLine1).find()) {
      String version = null;
      if (lddLine2 != null) {
        String[] parts = lddLine2.trim().split("\\s+");
        if (parts.length > 1) {
          version = parts[1];
        }
      }
      return new LibcInfo(MUSL, version);
    }
    return null;
  }

  /**
   * Runs {@code getconf GNU_LIBC_VERSION} and {@code ldd --version} via {@code /bin/sh} and returns
   * the combined stdout/stderr, or {@code null} on failure.
   */
  private static String runLibcVersionCommands() {
    ProcessBuilder pb =
        new ProcessBuilder(
            "/bin/sh", "-c", "getconf GNU_LIBC_VERSION 2>&1 || true; ldd --version 2>&1 || true");
    pb.redirectErrorStream(true);
    Process process = null;
    try {
      process = pb.start();
      String output = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
      if (!process.waitFor(EXEC_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        process.destroyForcibly();
        logger.debug("Libc version command timed out after {}ms", EXEC_TIMEOUT_MS);
        return null;
      }
      return output;
    } catch (IOException e) {
      logger.debug("Failed to run libc version command: {}", e.getMessage());
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.debug("Interrupted while running libc version command: {}", e.getMessage());
      return null;
    } catch (Exception e) {
      logger.debug("Unexpected error running libc version command: {}", e.getMessage());
      return null;
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  /** Visible for testing. Resets the cache so that {@link #load()} re-detects on next call. */
  static synchronized void resetCacheForTesting() {
    cachedResult = null;
  }
}
