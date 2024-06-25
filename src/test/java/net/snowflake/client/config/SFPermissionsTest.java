package net.snowflake.client.config;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnWin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SFPermissionsTest {

  @Parameterized.Parameters(name = "permission={0}")
  public static Set<Map.Entry<String, Boolean>> data() {
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
    return testConfigFilePermissions.entrySet();
  }

  Path configFilePath = Paths.get("config.json");
  String configJson = "{\"common\":{\"log_level\":\"debug\",\"log_path\":\"logs\"}}";
  String permission;
  Boolean isSucceed;

  public SFPermissionsTest(Map.Entry<String, Boolean> permission) {
    this.permission = permission.getKey();
    this.isSucceed = permission.getValue();
  }

  @Before
  public void createConfigFile() throws IOException {
    Files.write(configFilePath, configJson.getBytes());
  }

  @After
  public void cleanupConfigFile() throws IOException {
    Files.deleteIfExists(configFilePath);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnWin.class)
  public void testLogDirectoryPermissions() throws IOException {
    // TODO: SNOW-1503722 Change to check for thrown exceptions
    // Don't run on Windows
    Files.setPosixFilePermissions(configFilePath, PosixFilePermissions.fromString(permission));
    Boolean result = SFClientConfigParser.checkConfigFilePermissions(configFilePath.toString());
    if (isSucceed != result) {
      fail("testLogDirectoryPermissions failed. Expected " + isSucceed);
    }
  }
}
