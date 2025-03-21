package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import net.snowflake.client.annotations.DontRunOnWindows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SFPermissionsTest {
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
  @CsvSource({
    "rwx------,false",
    "rw-------,false",
    "r-x------,false",
    "r--------,false",
    "rwxrwx---,true",
    "rwxrw----,true",
    "rwxr-x---,false",
    "rwxr-----,false",
    "rwx-wx---,true",
    "rwx-w----,true",
    "rwx--x---,false",
    "rwx---rwx,true",
    "rwx---rw-,true",
    "rwx---r-x,false",
    "rwx---r--,false",
    "rwx----wx,true",
    "rwx----w-,true",
    "rwx-----x,false"
  })
  @DontRunOnWindows
  public void testLogDirectoryPermissions(String permission, boolean isSucceed) throws IOException {
    // TODO: SNOW-1503722 Change to check for thrown exceptions
    // Don't run on Windows
    Files.setPosixFilePermissions(configFilePath, PosixFilePermissions.fromString(permission));
    Boolean result =
        SFClientConfigParser.checkGroupOthersWritePermissions(configFilePath.toString());
    assertEquals(isSucceed, result);
  }
}
