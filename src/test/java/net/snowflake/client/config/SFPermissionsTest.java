package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.stream.Stream;
import net.snowflake.client.annotations.DontRunOnWindows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class SFPermissionsTest {

  static class PermissionProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("rwx------", false),
          Arguments.of("rw-------", false),
          Arguments.of("r-x------", false),
          Arguments.of("r--------", false),
          Arguments.of("rwxrwx---", true),
          Arguments.of("rwxrw----", true),
          Arguments.of("rwxr-x---", false),
          Arguments.of("rwxr-----", false),
          Arguments.of("rwx-wx---", true),
          Arguments.of("rwx-w----", true),
          Arguments.of("rwx--x---", false),
          Arguments.of("rwx---rwx", true),
          Arguments.of("rwx---rw-", true),
          Arguments.of("rwx---r-x", false),
          Arguments.of("rwx---r--", false),
          Arguments.of("rwx----wx", true),
          Arguments.of("rwx----w-", true),
          Arguments.of("rwx-----x", false));
    }
  }

  Path configFilePath = Paths.get("config.json");
  String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";

  @BeforeEach
  public void createConfigFile() throws IOException {
    Files.write(configFilePath, configJson.getBytes());
  }

  @AfterEach
  public void cleanupConfigFile() throws IOException {
    Files.deleteIfExists(configFilePath);
  }

  @ParameterizedTest
  @ArgumentsSource(PermissionProvider.class)
  @DontRunOnWindows
  public void testLogDirectoryPermissions(String permission, boolean isSucceed) throws IOException {
    // TODO: SNOW-1503722 Change to check for thrown exceptions
    // Don't run on Windows
    Files.setPosixFilePermissions(configFilePath, PosixFilePermissions.fromString(permission));
    Boolean result =
        SFClientConfigParser.checkGroupOthersWritePermissions(configFilePath.toString());
    if (isSucceed != result) {
      fail("testLogDirectoryPermissions failed. Expected " + isSucceed);
    }
  }
}
