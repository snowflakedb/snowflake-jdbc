package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link DownloadPathValidator}.
 *
 * <p>Verifies the canonical-path defense-in-depth check used by the cloud storage clients to
 * prevent a server-supplied destination filename from escaping the user's GET target directory via
 * {@code "../"} segments, absolute paths, or symlink redirection.
 */
public class DownloadPathValidatorTest {

  private static final String QUERY_ID = "test-query-id";

  @Test
  public void filenameInsideTargetDirectoryIsAccepted(@TempDir Path tmp) {
    File dest = tmp.resolve("data.csv").toFile();
    assertDoesNotThrow(
        () -> DownloadPathValidator.assertWithinDirectory(tmp.toString(), dest, QUERY_ID));
  }

  @Test
  public void nestedFilenameIsAccepted(@TempDir Path tmp) throws IOException {
    Files.createDirectories(tmp.resolve("a/b"));
    File dest = tmp.resolve("a/b/data.csv").toFile();
    assertDoesNotThrow(
        () -> DownloadPathValidator.assertWithinDirectory(tmp.toString(), dest, QUERY_ID));
  }

  @Test
  public void parentDirectoryEscapeIsRejected(@TempDir Path tmp) throws IOException {
    Path subdir = Files.createDirectory(tmp.resolve("dl"));
    File dest = subdir.resolve("../escaped.bin").toFile();
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(subdir.toString(), dest, QUERY_ID));
  }

  @Test
  public void siblingDirectoryWithSamePrefixIsRejected(@TempDir Path tmp) throws IOException {
    Path subdir = Files.createDirectory(tmp.resolve("dl"));
    Files.createDirectory(tmp.resolve("dl-other"));
    File dest = tmp.resolve("dl-other/file.bin").toFile();
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(subdir.toString(), dest, QUERY_ID));
  }

  @Test
  public void absolutePathOutsideTargetIsRejected(@TempDir Path tmp) {
    File dest = new File(System.getProperty("java.io.tmpdir"), "evil.bin");
    Path subdir = tmp.resolve("dl");
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(subdir.toString(), dest, QUERY_ID));
  }

  /**
   * Reproduces the SNOW-3437355 attack vector at the storage-client layer: even if a future
   * regression let a malicious destFileName containing {@code "..\\"} sequences slip through to the
   * {@code File} constructor on Windows, the validator still rejects it.
   */
  @Test
  public void windowsTraversalFilenameIsRejected(@TempDir Path tmp) {
    assumeTrue(File.separatorChar == '\\', "Windows-only behavior of backslash separator");
    File dest = new File(tmp.toFile(), "..\\..\\evil.dll");
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(tmp.toString(), dest, QUERY_ID));
  }

  /**
   * POSIX-only: on Windows, {@link File#getCanonicalPath()} does not resolve a parent symlink when
   * the leaf segment does not exist on disk, so the canonical form of {@code
   * <tmp>\inside\link\evil.bin} stays inside {@code <tmp>\inside} and the validator (correctly per
   * the JDK contract) cannot detect the escape until the file is materialized. Windows symlink
   * creation also requires elevated privileges and behaves differently from POSIX (junction vs
   * symlink distinction, separate resolution rules), so the symlink-escape guarantee is documented
   * and verified on POSIX only. Other escape vectors on Windows (parent traversal, absolute paths,
   * drive-letter forms, ADS) are exercised by separate tests.
   */
  @Test
  public void symlinkEscapeIsRejected(@TempDir Path tmp) throws IOException {
    assumeTrue(
        File.separatorChar == '/',
        "Symlink escape detection relies on POSIX getCanonicalPath() semantics");
    Path inside = Files.createDirectory(tmp.resolve("inside"));
    Path outside = Files.createDirectory(tmp.resolve("outside"));
    Path link = inside.resolve("link");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (UnsupportedOperationException | IOException e) {
      assumeTrue(false, "Filesystem does not support symlinks: " + e.getMessage());
    }
    File dest = link.resolve("evil.bin").toFile();
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(inside.toString(), dest, QUERY_ID));
  }

  @Test
  public void nullLocalLocationIsRejected(@TempDir Path tmp) {
    File dest = tmp.resolve("data.csv").toFile();
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(null, dest, QUERY_ID));
  }

  @Test
  public void nullDestFileIsRejected(@TempDir Path tmp) {
    assertThrows(
        SnowflakeSQLException.class,
        () -> DownloadPathValidator.assertWithinDirectory(tmp.toString(), null, QUERY_ID));
  }

  @Test
  public void targetDirectoryReferencingItselfIsAccepted(@TempDir Path tmp) {
    Path subdir = tmp.resolve("dl");
    File dest = Paths.get(subdir.toString(), ".", "file.csv").toFile();
    assertDoesNotThrow(
        () -> DownloadPathValidator.assertWithinDirectory(subdir.toString(), dest, QUERY_ID));
  }

  /**
   * Exercises the {@code catch (IOException | InvalidPathException)} wrapping branch. A NUL byte in
   * the destination filename is rejected by {@link File#getCanonicalFile()} on every supported OS
   * (the underlying syscall treats NUL as the C end-of-string marker), and on JDK 8+ a NUL in a
   * path also makes {@link File#toPath()} throw {@link java.nio.file.InvalidPathException}. Either
   * way the validator must wrap the failure as a {@link SnowflakeSQLException} with the original
   * exception preserved as the cause — without this test the wrapping wiring (queryId, SqlState,
   * vendor code, cause) would be unverified by CI.
   */
  @Test
  public void canonicalizationFailureIsWrappedAsSnowflakeSQLException(@TempDir Path tmp) {
    File dest = new File(tmp.toFile(), "evil\u0000.bin");
    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () -> DownloadPathValidator.assertWithinDirectory(tmp.toString(), dest, QUERY_ID));
    assertNotNull(ex.getCause(), "Original IOException/InvalidPathException must be preserved");
    assertTrue(
        ex.getCause() instanceof IOException || ex.getCause() instanceof InvalidPathException,
        "Cause should be IOException or InvalidPathException, was: " + ex.getCause().getClass());
  }
}
