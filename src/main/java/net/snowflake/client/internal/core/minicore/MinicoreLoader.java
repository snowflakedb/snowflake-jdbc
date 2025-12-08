package net.snowflake.client.internal.core.minicore;

import com.sun.jna.Native;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class MinicoreLoader {

  private static final SFLogger logger = SFLoggerFactory.getLogger(MinicoreLoader.class);
  private static final String TEMP_FILE_PREFIX = "snowflake-minicore-";

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
      loadLogger.log("Platform not supported: " + platform.getPlatformIdentifier());
      loadResult =
          MinicoreLoadResult.failure(
              String.format(
                  "Unsupported platform: OS=%s, Arch=%s",
                  platform.getOsName(), platform.getOsArch()),
              platform.getLibraryFileName(),
              null,
              loadLogger.getLogs());
      return loadResult;
    }

    loadLogger.log("Platform supported: " + platform.getPlatformIdentifier());
    loadResult = loadLibraryFromJar(platform);
    return loadResult;
  }

  private MinicoreLoadResult loadLibraryFromJar(MinicorePlatform platform) {
    String resourcePath = platform.getLibraryPath();
    loadLogger.log("Library resource path: " + resourcePath);

    String libraryFileName = platform.getLibraryFileName();
    try (InputStream libraryStream = MinicoreLoader.class.getResourceAsStream(resourcePath)) {
      if (libraryStream == null) {
        loadLogger.log("Library resource not found in JAR");
        return MinicoreLoadResult.failure(
            String.format("Library resource not found in JAR: %s", resourcePath),
            libraryFileName,
            null,
            loadLogger.getLogs());
      }

      loadLogger.log("Library resource found in JAR");
      Path targetPath = determineTargetPath(libraryFileName);
      if (targetPath == null) {
        loadLogger.log("No writable directory found");
        return MinicoreLoadResult.failure(
            "No writable directory found (tried: system temp, home cache dir, working dir)",
            libraryFileName,
            null,
            loadLogger.getLogs());
      }

      return persistLoadAndDelete(targetPath, libraryStream, libraryFileName);
    } catch (IOException e) {
      loadLogger.log("Failed to read library from JAR: " + e.getMessage());
      return MinicoreLoadResult.failure(
          "Failed to read library from JAR: " + e.getMessage(),
          libraryFileName,
          e,
          loadLogger.getLogs());
    }
  }

  private Path determineTargetPath(String libraryFileName) {
    // 1. System temp directory (preferred - isolated, auto-cleaned)
    Path path = tryTempDirectory(libraryFileName);
    if (path != null) {
      return path;
    }

    // 2. OS-specific home cache directory
    path = tryHomeCacheDirectory(libraryFileName);
    if (path != null) {
      return path;
    }
    // 3. Current working directory (last resort)
    return tryWorkingDirectory(libraryFileName);
  }

  private Path tryTempDirectory(String libraryFileName) {
    try {
      Path tempDir = Files.createTempDirectory(TEMP_FILE_PREFIX);
      setPermissions700(tempDir);
      loadLogger.log("Using temp directory");
      return tempDir.resolve(libraryFileName);
    } catch (Exception e) {
      loadLogger.log("Failed to use temp directory: " + e.getMessage());
      return null;
    }
  }

  private Path tryHomeCacheDirectory(String libraryFileName) {
    try {
      Path cacheDir = getHomeCacheDirectory();
      if (cacheDir == null) {
        return null;
      }

      if (!Files.exists(cacheDir)) {
        Files.createDirectories(cacheDir);
        setPermissions700(cacheDir);
      }

      if (Files.isWritable(cacheDir)) {
        loadLogger.log("Using home cache directory: " + cacheDir);
        return cacheDir.resolve(libraryFileName);
      }
      loadLogger.log("Home cache directory not writable: " + cacheDir);
    } catch (Exception e) {
      loadLogger.log("Failed to use home cache directory: " + e.getMessage());
    }
    return null;
  }

  private Path tryWorkingDirectory(String libraryFileName) {
    try {
      String cwd = System.getProperty("user.dir");
      if (cwd != null && !cwd.isEmpty()) {
        Path cwdPath = Paths.get(cwd);
        if (Files.isWritable(cwdPath)) {
          loadLogger.log("Using current working directory");
          return cwdPath.resolve(TEMP_FILE_PREFIX + System.nanoTime() + "-" + libraryFileName);
        }
      }
    } catch (Exception e) {
      loadLogger.log("Failed to use working directory: " + e.getMessage());
    }
    return null;
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

    Constants.OS os = platform.getOs();
    if (os == Constants.OS.WINDOWS) {
      return Paths.get(home, "AppData", "Local", "Snowflake", "Caches", "minicore");
    } else if (os == Constants.OS.MAC) {
      return Paths.get(home, "Library", "Caches", "Snowflake", "minicore");
    } else {
      return Paths.get(home, ".cache", "Snowflake", "minicore");
    }
  }

  private MinicoreLoadResult persistLoadAndDelete(
      Path targetPath, InputStream libraryStream, String libraryFileName) {
    try {
      MinicoreLibrary loadedLibrary = loadLibraryFromDisk(targetPath, libraryStream);
      String coreVersion = getLibraryVersion(loadedLibrary);

      return MinicoreLoadResult.success(
          libraryFileName, loadedLibrary, coreVersion, loadLogger.getLogs());
    } catch (LoadLibraryException e) {
      return MinicoreLoadResult.failure(
          e.getMessage(), libraryFileName, e.getCause(), loadLogger.getLogs());
    } finally {
      deleteLibraryFile(targetPath);
      loadLogger.log("Deleted temporary library file");
    }
  }

  private MinicoreLibrary loadLibraryFromDisk(Path targetPath, InputStream libraryStream)
      throws LoadLibraryException {
    try {
      persistLibraryToDisk(targetPath, libraryStream);
      loadLogger.log("Loading minicore library");
      MinicoreLibrary library = loadLibraryFromPath(targetPath);
      loadLogger.log("Minicore library loaded successfully");
      return library;
    } catch (IOException e) {
      loadLogger.log("Failed to persist library: " + e.getMessage());
      throw new LoadLibraryException("Failed to persist library: " + e.getMessage(), e);
    } catch (UnsatisfiedLinkError e) {
      loadLogger.log("Failed to load library: " + e.getMessage());
      throw new LoadLibraryException("Failed to load library: " + e.getMessage(), e);
    } catch (Exception e) {
      loadLogger.log("Unexpected error loading library: " + e.getMessage());
      throw new LoadLibraryException("Unexpected error loading library: " + e.getMessage(), e);
    }
  }

  private String getLibraryVersion(MinicoreLibrary library) throws LoadLibraryException {
    try {
      loadLogger.log("Calling sf_core_full_version");
      String version = library.sf_core_full_version();
      loadLogger.log("sf_core_full_version returned: " + version);
      return version;
    } catch (UnsatisfiedLinkError e) {
      loadLogger.log("Failed to load symbol sf_core_full_version: " + e.getMessage());
      throw new LoadLibraryException(
          "Failed to load symbol sf_core_full_version: " + e.getMessage(), e);
    } catch (Exception e) {
      loadLogger.log("Unexpected error calling sf_core_full_version: " + e.getMessage());
      throw new LoadLibraryException(
          "Unexpected error calling sf_core_full_version: " + e.getMessage(), e);
    }
  }

  /** Internal exception for library loading failures. */
  private static class LoadLibraryException extends Exception {
    LoadLibraryException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private void persistLibraryToDisk(Path targetPath, InputStream libraryStream) throws IOException {
    loadLogger.log("Writing embedded library to disk");
    Files.copy(libraryStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
    loadLogger.log("Successfully wrote embedded library");
    loadLogger.log("Setting file permissions to 600");
    setPermissions600(targetPath);
    loadLogger.log("File permissions configured");
  }

  private MinicoreLibrary loadLibraryFromPath(Path libraryPath) throws UnsatisfiedLinkError {
    String absolutePath = libraryPath.toAbsolutePath().toString();
    loadLogger.log("Calling Native.load");
    MinicoreLibrary lib = Native.load(absolutePath, MinicoreLibrary.class);
    loadLogger.log("Native.load finished");
    return lib;
  }

  private void deleteLibraryFile(Path filePath) {
    try {
      Files.deleteIfExists(filePath);
    } catch (IOException e) {
      logger.trace("Failed to delete library file: {}", e.getMessage());
    }
  }

  private void setPermissions700(Path path) throws IOException {
    setPermissions(path, true);
  }

  private void setPermissions600(Path path) throws IOException {
    setPermissions(path, false);
  }

  private void setPermissions(Path path, boolean executable) throws IOException {
    if (platform.getOs().equals(Constants.OS.WINDOWS)) {
      // Windows doesn't support POSIX permissions
      // Use basic file attributes instead
      path.toFile().setReadable(true, true); // readable by owner only
      path.toFile().setWritable(true, true); // writable by owner only
      path.toFile().setExecutable(executable, true); // executable by owner only if needed
    } else {
      Set<PosixFilePermission> perms = new HashSet<>();
      perms.add(PosixFilePermission.OWNER_READ);
      perms.add(PosixFilePermission.OWNER_WRITE);
      if (executable) {
        perms.add(PosixFilePermission.OWNER_EXECUTE);
      }
      Files.setPosixFilePermissions(path, perms);
    }
  }
}
