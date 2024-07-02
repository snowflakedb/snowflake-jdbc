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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SFPermissionsTest {
  @Rule public ConditionalIgnoreRule rule = new ConditionalIgnoreRule();

  @Parameterized.Parameters(name = "permission={0}")
  public static Set<Map.Entry<String, Boolean>> data() {
    Map<String, Boolean> testConfigFilePermissions =
        new HashMap<String, Boolean>() {
          {
            put("rwx------", false);
            put("rw-------", false);
            put("r-x------", false);
            put("r--------", false);
            put("rwxrwx---", true);
            put("rwxrw----", true);
            put("rwxr-x---", false);
            put("rwxr-----", false);
            put("rwx-wx---", true);
            put("rwx-w----", true);
            put("rwx--x---", false);
            put("rwx---rwx", true);
            put("rwx---rw-", true);
            put("rwx---r-x", false);
            put("rwx---r--", false);
            put("rwx----wx", true);
            put("rwx----w-", true);
            put("rwx-----x", false);
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
    Boolean result =
        SFClientConfigParser.checkGroupOthersWritePermissions(configFilePath.toString());
    if (isSucceed != result) {
      fail("testLogDirectoryPermissions failed. Expected " + isSucceed);
    }
  }
}
