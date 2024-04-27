package net.snowflake.client.config;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.core.Constants;
import org.junit.Test;

public class SFPermissionsTest {

  Map<String, Boolean> testConfigFilePermissions =
      new HashMap<String, Boolean>() {
        {
          put("rwx------", true);
          put("rw-------", true);
          put("r-x------", true);
          put("r--------", true);
          put("rwxrwx---", false);
          put("rwxrw----", false);
          put("rwxr-x---", true);
          put("rwxr-----", true);
          put("rwx-wx---", false);
          put("rwx-w----", false);
          put("rwx--x---", true);
          put("rwx---rwx", false);
          put("rwx---rw-", false);
          put("rwx---r-x", true);
          put("rwx---r--", true);
          put("rwx----wx", false);
          put("rwx----w-", false);
          put("rwx-----x", true);
        }
      };

  @Test
  public void testLogDirectoryPermissions() {
    if (Constants.getOS() != Constants.OS.WINDOWS) {
      Path configFilePath = Paths.get("config.json");
      String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";
      try {
        Files.write(configFilePath, configJson.getBytes());
        for (Map.Entry<String, Boolean> entry : testConfigFilePermissions.entrySet()) {
          Files.setPosixFilePermissions(
              configFilePath, PosixFilePermissions.fromString(entry.getKey()));
          try {
            SFClientConfigParser.loadSFClientConfig(configFilePath.toString());
            if (!entry.getValue()) {
              fail("testLogDirectoryPermissions failed. Expected exception.");
            }
          } catch (IOException e) {
            if (entry.getValue()) {
              fail("testLogDirectoryPermissions failed. Expected pass.");
            }
          }
        }
        Files.deleteIfExists(configFilePath);
      } catch (IOException e) {
        fail("testLogDirectoryPermissions failed");
      }
    }
  }
}
