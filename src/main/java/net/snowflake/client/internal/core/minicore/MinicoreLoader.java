package net.snowflake.client.internal.core.minicore;

import com.sun.jna.Native;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.commons.io.IOUtils;

@SnowflakeJdbcInternalApi
public class MinicoreLoader {

  private static final SFLogger logger = SFLoggerFactory.getLogger(MinicoreLoader.class);
  private static final String TEMP_DIR_PREFIX = "snowflake-minicore-";

  private MinicoreLoadResult loadResult;
  private final MinicoreLoadLogger loadLogger = new MinicoreLoadLogger();
  private final MinicorePlatform platform = new MinicorePlatform();

  public synchronized MinicoreLoadResult loadLibrary() {
    if (loadResult != null) {
      return loadResult;
    }

    loadLogger.log("Starting minicore loading");
    loadLogger.log(
        "Detected platform: OS=" + platform.getOsName() + ", Arch=" + platform.getOsArch());

    if (!platform.isSupported()) {
      loadLogger.log("Platform not supported");
      loadResult =
          failure(
              "Unsupported platform: OS=" + platform.getOsName() + ", Arch=" + platform.getOsArch(),
              null);
      return loadResult;
    }

    loadLogger.log("Platform supported: " + platform.getPlatformIdentifier());
    loadResult = loadFromJar();
    return loadResult;
  }

  private MinicoreLoadResult loadFromJar() {
    String resourcePath = platform.getLibraryPath();
    loadLogger.log("Library resource path: " + resourcePath);

    byte[] libraryBytes = readLibraryFromJar(resourcePath);
    if (libraryBytes == null) {
      return failure("Library resource not found in JAR: " + resourcePath, null);
    }

    MinicoreLoadResult result = tryDirectory("temp", libraryBytes, this::createTempDirectory);
    if (result != null) {
      return result;
    }

    result = tryDirectory("home cache", libraryBytes, this::getOrCreateHomeCacheDirectory);
    if (result != null) {
      return result;
    }

    result = tryDirectory("working", libraryBytes, this::getWorkingDirectory);
    if (result != null) {
      return result;
    }

    loadLogger.log("No writable directory found");
    return failure("No writable directory found (tried: temp, home cache, working dir)", null);
  }

  private byte[] readLibraryFromJar(String resourcePath) {
    try (InputStream stream = MinicoreLoader.class.getResourceAsStream(resourcePath)) {
      if (stream == null) {
        loadLogger.log("Library resource not found in JAR");
        return null;
      }
      loadLogger.log("Library resource found in JAR");
      return IOUtils.toByteArray(stream);
    } catch (IOException e) {
      loadLogger.log("Failed to read library from JAR: " + e.getMessage());
      return null;
    }
  }

  private MinicoreLoadResult tryDirectory(
      String name, byte[] libraryBytes, DirectorySupplier supplier) {
    Path targetPath = null;
    Path createdTempDir = null;
    try {
      Path directory = supplier.get();
      if (directory == null) {
        return null;
      }
      // Track if this is a temp directory we created (so we can clean it up on failure)
      if (directory.toString().startsWith(System.getProperty("java.io.tmpdir"))) {
        createdTempDir = directory;
      }

      targetPath = directory.resolve(platform.getLibraryFileName());
      loadLogger.log("Trying " + name + " directory: " + directory);
      return writeLoadAndCleanup(targetPath, libraryBytes);
    } catch (Exception e) {
      loadLogger.log("Failed to use " + name + " directory: " + e.getMessage());
      cleanup(targetPath, createdTempDir);
      return null;
    }
  }

  private Path createTempDirectory() throws IOException {
    Path tempDir = Files.createTempDirectory(TEMP_DIR_PREFIX);
    setDirectoryPermissions(tempDir);
    return tempDir;
  }

  private Path getOrCreateHomeCacheDirectory() throws IOException {
    Path cacheDir = getHomeCacheDirectory();
    if (cacheDir == null) {
      return null;
    }
    if (!Files.exists(cacheDir)) {
      Files.createDirectories(cacheDir);
    }
    // Always ensure correct permissions (may have been created by another process)
    setDirectoryPermissions(cacheDir);
    return cacheDir;
  }

  private Path getWorkingDirectory() {
    String cwd = System.getProperty("user.dir");
    return (cwd != null && !cwd.isEmpty()) ? Paths.get(cwd) : null;
  }

  /**
   * Returns the OS-specific cache directory path:
   *
   * <ul>
   *   <li>Windows: %USERPROFILE%/AppData/Local/Snowflake/Caches/minicore/
   *   <li>MacOS: $HOME/Library/Caches/Snowflake/minicore/
   *   <li>Other: $HOME/.cache/Snowflake/minicore/
   * </ul>
   */
  Path getHomeCacheDirectory() {
    String home = System.getProperty("user.home");
    if (home == null || home.isEmpty()) {
      return null;
    }

    switch (platform.getOs()) {
      case WINDOWS:
        return Paths.get(home, "AppData", "Local", "Snowflake", "Caches", "minicore");
      case MAC:
        return Paths.get(home, "Library", "Caches", "Snowflake", "minicore");
      default:
        return Paths.get(home, ".cache", "Snowflake", "minicore");
    }
  }

  private MinicoreLoadResult writeLoadAndCleanup(Path targetPath, byte[] libraryBytes)
      throws IOException {
    Files.write(targetPath, libraryBytes);
    loadLogger.log("Wrote library to: " + targetPath);
    setFilePermissions(targetPath);

    try {
      loadLogger.log("Loading library");
      MinicoreLibrary library =
          Native.load(targetPath.toAbsolutePath().toString(), MinicoreLibrary.class);
      loadLogger.log("Library loaded successfully");

      return getVersionAndCreateResult(library);
    } finally {
      deleteQuietly(targetPath);
      loadLogger.log("Deleted library file");
    }
  }

  private MinicoreLoadResult getVersionAndCreateResult(MinicoreLibrary library) {
    try {
      String version = library.sf_core_full_version();
      loadLogger.log("Library version: " + version);
      return MinicoreLoadResult.success(
          platform.getLibraryFileName(), library, version, loadLogger.getLogs());
    } catch (UnsatisfiedLinkError e) {
      loadLogger.log("Library missing sf_core_full_version symbol: " + e.getMessage());
      return failure("Library missing required symbol: sf_core_full_version", e);
    } catch (Exception e) {
      loadLogger.log("Failed to get library version: " + e.getMessage());
      return failure("Failed to get library version: " + e.getMessage(), e);
    }
  }

  private void cleanup(Path filePath, Path tempDirectory) {
    deleteQuietly(filePath);
    deleteQuietly(tempDirectory);
  }

  private void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      logger.trace("Failed to delete: {}", e.getMessage());
    }
  }

  private MinicoreLoadResult failure(String message, Throwable cause) {
    return MinicoreLoadResult.failure(
        message, platform.getLibraryFileName(), cause, loadLogger.getLogs());
  }

  private void setDirectoryPermissions(Path path) throws IOException {
    setPermissions(path, true);
  }

  private void setFilePermissions(Path path) throws IOException {
    setPermissions(path, false);
  }

  private void setPermissions(Path path, boolean executable) throws IOException {
    if (platform.getOs() == Constants.OS.WINDOWS) {
      path.toFile().setReadable(true, true);
      path.toFile().setWritable(true, true);
      path.toFile().setExecutable(executable, true);
    } else {
      EnumSet<PosixFilePermission> perms =
          EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
      if (executable) {
        perms.add(PosixFilePermission.OWNER_EXECUTE);
      }
      Files.setPosixFilePermissions(path, perms);
    }
  }

  @FunctionalInterface
  private interface DirectorySupplier {
    Path get() throws IOException;
  }
}
