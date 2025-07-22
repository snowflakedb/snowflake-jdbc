package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import net.snowflake.client.annotations.DontRunOnWindows;
import org.junit.jupiter.api.Test;

class SFConnectionConfigParserPermissionTest {

  private Path createTempFileWithPermissions(Set<PosixFilePermission> perms) throws Exception {
    Path tempFile = Files.createTempFile("testConfig", ".toml");
    Files.setPosixFilePermissions(tempFile, perms);
    return tempFile;
  }

  private void invokeVerifyFilePermissionSecure(Path path) throws Exception {
    Method method =
        SFConnectionConfigParser.class.getDeclaredMethod("verifyFilePermissionSecure", Path.class);
    method.setAccessible(true);
    try {
      method.invoke(null, path);
    } catch (java.lang.reflect.InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof net.snowflake.client.jdbc.SnowflakeSQLException) {
        throw (net.snowflake.client.jdbc.SnowflakeSQLException) cause;
      } else if (cause instanceof Exception) {
        throw (Exception) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  @Test
  @DontRunOnWindows
  void testGroupWritePermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_WRITE);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));
    System.out.println("Exception message: " + ex.getMessage());

    assertTrue(ex.getMessage().contains("writable by group or others"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testOthersWritePermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OTHERS_WRITE);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));

    System.out.println("Exception message: " + ex.getMessage());

    assertTrue(ex.getMessage().contains("writable by group or others"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testOwnerExecutePermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));
    assertTrue(ex.getMessage().contains("safe because file permissions are different"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testGroupExecutePermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_EXECUTE);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));
    assertTrue(ex.getMessage().contains("safe because file permissions are different"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testOthersExecutePermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OTHERS_EXECUTE);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));
    assertTrue(ex.getMessage().contains("safe because file permissions are different"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testGroupReadPermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_READ);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));
    assertTrue(ex.getMessage().contains("safe because file permissions are different"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testOthersReadPermissionRaisesException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OTHERS_READ);
    Path tempFile = createTempFileWithPermissions(perms);
    Exception ex =
        assertThrows(
            net.snowflake.client.jdbc.SnowflakeSQLException.class,
            () -> invokeVerifyFilePermissionSecure(tempFile));
    assertTrue(ex.getMessage().contains("safe because file permissions are different"));
    Files.deleteIfExists(tempFile);
  }

  @Test
  @DontRunOnWindows
  void testOwnerReadWritePermissionDoesNotRaiseException() throws Exception {
    Set<PosixFilePermission> perms =
        Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
    Path tempFile = createTempFileWithPermissions(perms);
    assertDoesNotThrow(() -> invokeVerifyFilePermissionSecure(tempFile));
    Files.deleteIfExists(tempFile);
  }
}
