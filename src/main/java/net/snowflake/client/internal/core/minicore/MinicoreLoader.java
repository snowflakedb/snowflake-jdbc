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

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class MinicoreLoader {

  private static final SFLogger logger = SFLoggerFactory.getLogger(MinicoreLoader.class);
  private static final String TEMP_FILE_PREFIX = "snowflake-minicore-";

  private volatile MinicoreLoadResult loadResult;
  private volatile MinicoreLibrary library;
  private final MinicoreLoadLogger loadLogger;

  public MinicoreLoader() {
    this.loadLogger = new MinicoreLoadLogger();
  }

  public synchronized MinicoreLoadResult loadLibrary() {
    if (loadResult != null) {
      return loadResult;
    }
    
    loadLogger.log("Starting minicore loading");
    
    MinicorePlatform platform = new MinicorePlatform();
    loadLogger.log("Detected platform: OS=" + platform.getOsName() + ", Arch=" + platform.getOsArch());
    
    if (!platform.isSupported()) {
      loadLogger.log("Platform not supported: "+ platform.getPlatformIdentifier());
      loadResult =
          MinicoreLoadResult.failure(
              String.format(
                  "Unsupported platform: OS=%s, Arch=%s",
                  platform.getOsName(), platform.getOsArch()),
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
    
    try (InputStream libraryStream = MinicoreLoader.class.getResourceAsStream(resourcePath)) {
      if (libraryStream == null) {
        loadLogger.log("Library resource not found in JAR");
        return MinicoreLoadResult.failure(
            String.format("Library resource not found in JAR: %s", resourcePath), null, loadLogger.getLogs());
      }

      loadLogger.log("Library resource found in JAR");
      Path targetPath = determineTargetPath(platform.getLibraryFileName());
      if (targetPath == null) {
        loadLogger.log("No writable directory found");
        return MinicoreLoadResult.failure(
            "No writable directory found (tried: system temp, working dir, home dir)", null, loadLogger.getLogs());
      }

      return persistLoadAndDelete(targetPath, libraryStream);
    } catch (IOException e) {
      loadLogger.log("Failed to read library from JAR: " + e.getMessage());
      return MinicoreLoadResult.failure(
          "Failed to read library from JAR: " + e.getMessage(), e, loadLogger.getLogs());
    }
  }

  private Path determineTargetPath(String libraryFileName) {
    // Try system temp directory first (create dedicated temp directory)
    try {
      Path tempDir = Files.createTempDirectory(TEMP_FILE_PREFIX);
      loadLogger.log("Created temp directory: " + tempDir);
      setPermissions700(tempDir);
      loadLogger.log("Configured temp directory permissions to 0700");
      Path filePath = tempDir.resolve(libraryFileName);
      return filePath;
    } catch (Exception e) {
      loadLogger.log("Failed to create system temp directory: " + e.getMessage());
    }

    // Fallback to working directory
    Path path = tryDirectoryForFile("user.dir", libraryFileName, "working");
    if (path != null) return path;

    // Fallback to home directory
    path = tryDirectoryForFile("user.home", libraryFileName, "home");
    if (path != null) return path;

    return null;
  }

  private Path tryDirectoryForFile(String propertyName, String libraryFileName, String dirType) {
    try {
      String dir = System.getProperty(propertyName);
      if (dir != null && !dir.isEmpty()) {
        Path baseDir = Paths.get(dir);
        if (Files.isWritable(baseDir)) {
          Path filePath = baseDir.resolve(TEMP_FILE_PREFIX + System.nanoTime() + "-" + libraryFileName);
          loadLogger.log("Using " + dirType + " directory: " + baseDir);
          return filePath;
        }
      }
    } catch (Exception e) {
      loadLogger.log("Failed to use " + dirType + " directory: " + e.getMessage());
    }
    return null;
  }


  private MinicoreLoadResult persistLoadAndDelete(
      Path targetPath, InputStream libraryStream) {
    try {
      persistLibraryToDisk(targetPath, libraryStream);
      loadLogger.log("Loading minicore library from: " + targetPath);
      MinicoreLibrary loadedLibrary = loadLibraryFromPath(targetPath);
      this.library = loadedLibrary;
      loadLogger.log("Minicore library loaded successfully");
      return MinicoreLoadResult.success(targetPath.toAbsolutePath().toString(), loadLogger.getLogs());
    } catch (IOException e) {
      loadLogger.log("Failed to persist library: " + e.getMessage());
      return MinicoreLoadResult.failure(
          String.format("Failed to persist library to %s: %s", targetPath, e.getMessage()), e, loadLogger.getLogs());
    } catch (UnsatisfiedLinkError e) {
      loadLogger.log("Failed to load library: " + e.getMessage());
      return MinicoreLoadResult.failure(
          String.format("Failed to load library from %s: %s", targetPath, e.getMessage()), e, loadLogger.getLogs());
    } catch (Exception e) {
      loadLogger.log("Unexpected error: " + e.getMessage());
      return MinicoreLoadResult.failure(
          String.format("Unexpected error with library at %s: %s", targetPath, e.getMessage()), e, loadLogger.getLogs());
    } finally {
      deleteLibraryFile(targetPath);
      loadLogger.log("Deleted temporary library file");
    }
  }

  private Path persistLibraryToDisk(Path targetPath, InputStream libraryStream)
      throws IOException {
    loadLogger.log("Writing embedded library to: " + targetPath);
    Files.copy(libraryStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
    loadLogger.log("Successfully wrote embedded library");
    loadLogger.log("Setting file permissions to 600");
    setPermissions600(targetPath);
    loadLogger.log("File permissions configured");
    return targetPath;
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
      logger.trace("Failed to delete library file {}: {}", filePath, e.getMessage());
    }
  }

  private void setPermissions700(Path path) throws IOException {
    setPermissions(path, true);
  }

  private void setPermissions600(Path path) throws IOException {
    setPermissions(path, false);
  }

  private void setPermissions(Path path, boolean executable) throws IOException {
    try {
      Set<PosixFilePermission> perms = new HashSet<>();
      perms.add(PosixFilePermission.OWNER_READ);
      perms.add(PosixFilePermission.OWNER_WRITE);
      if (executable) {
        perms.add(PosixFilePermission.OWNER_EXECUTE);
      }
      Files.setPosixFilePermissions(path, perms);
    } catch (UnsupportedOperationException e) {
      // Windows doesn't support POSIX permissions
      // Use basic file attributes instead
      path.toFile().setReadable(true, true); // readable by owner only
      path.toFile().setWritable(true, true); // writable by owner only
      path.toFile().setExecutable(executable, true); // executable by owner only if needed
    }
  }

  public MinicoreLibrary getLibrary() {
    return library;
  }
}
