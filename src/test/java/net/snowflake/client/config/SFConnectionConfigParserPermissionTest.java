package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.snowflake.client.annotations.DontRunOnWindows;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

  static List<Object[]> permissionTestCases() {
    return Arrays.asList(
        new Object[][] {
          { // Group write
            new HashSet<>(
                Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_WRITE)),
            true,
            "writable by group or others"
          },
          { // Others write
            new HashSet<>(
                Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OTHERS_WRITE)),
            true,
            "writable by group or others"
          },
          { // Owner execute
            new HashSet<>(
                Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE)),
            true,
            "executable"
          },
          { // Group execute
            new HashSet<>(
                Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_EXECUTE)),
            true,
            "executable"
          },
          { // Others execute
            new HashSet<>(
                Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OTHERS_EXECUTE)),
            true,
            "executable"
          },
          { // Owner read/write only
            new HashSet<>(
                Arrays.asList(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)),
            false,
            null
          }
        });
  }

  @ParameterizedTest
  @MethodSource("permissionTestCases")
  @DontRunOnWindows
  void testFilePermissionScenarios(
      Set<PosixFilePermission> perms, boolean shouldThrow, String expectedMsg) throws Exception {
    Path tempFile = createTempFileWithPermissions(perms);
    try {
      if (shouldThrow) {
        Exception ex =
            assertThrows(
                net.snowflake.client.jdbc.SnowflakeSQLException.class,
                () -> invokeVerifyFilePermissionSecure(tempFile));
        assertTrue(ex.getMessage().contains(expectedMsg));
      } else {
        assertDoesNotThrow(() -> invokeVerifyFilePermissionSecure(tempFile));
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  static List<Object[]> skipReadWarningTestCases() {
    return Arrays.asList(
        new Object[][] {
          {
            new HashSet<>(
                Arrays.asList(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.OTHERS_READ))
          }
        });
  }

  @ParameterizedTest
  @MethodSource("skipReadWarningTestCases")
  @DontRunOnWindows
  void testSkipWarningForReadPermissionsEnvVar(Set<PosixFilePermission> perms) throws Exception {
    SnowflakeUtil.systemSetEnv("SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE", "true");
    Path tempFile = createTempFileWithPermissions(perms);
    try {
      assertDoesNotThrow(() -> invokeVerifyFilePermissionSecure(tempFile));
    } finally {
      Files.deleteIfExists(tempFile);
      SnowflakeUtil.systemSetEnv("SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE", null);
    }
  }
}
