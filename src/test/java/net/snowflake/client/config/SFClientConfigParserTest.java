package net.snowflake.client.config;

import static net.snowflake.client.config.SFClientConfigParser.*;
import static net.snowflake.client.jdbc.SnowflakeUtil.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

public class SFClientConfigParserTest {
  private static final String CONFIG_JSON =
      "{\"common\":{\"log_level\":\"info\",\"log_path\":\"/jdbc.log\"}}";

  @Test
  public void testloadSFClientConfigValidPath() {
    Path configFilePath = Paths.get("config.json");
    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig =
          SFClientConfigParser.loadSFClientConfig(configFilePath.toString());
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());

      Files.delete(configFilePath);
    } catch (IOException e) {
      fail("testloadSFClientConfigValidPath failed");
    }
  }

  @Test
  public void testloadSFClientConfigInValidPath() {
    String configFilePath = "InvalidPath";
    SFClientConfig config = null;
    try {
      SFClientConfigParser.loadSFClientConfig(configFilePath.toString());
      fail("testloadSFClientConfigInValidPath"); // this will not be reached!
    } catch (IOException e) {
      // do nothing
    }
  }

  @Test
  public void testloadSFClientConfigInValidJson() {
    try {
      String invalidJson = "invalidJson";
      Path configFilePath = Paths.get("config.json");
      Files.write(configFilePath, invalidJson.getBytes());
      SFClientConfigParser.loadSFClientConfig(configFilePath.toString());

      fail("testloadSFClientConfigInValidJson");
    } catch (IOException e) {
      // DO Nothing
    }
  }

  @Test
  public void testloadSFClientConfigWithEnvVar() {
    Path configFilePath = Paths.get("config.json");

    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      systemSetEnv(SF_CLIENT_CONFIG_ENV_NAME, "config.json");
      SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());

      Files.delete(configFilePath);
      systemUnsetEnv(SF_CLIENT_CONFIG_ENV_NAME);
    } catch (IOException e) {
      fail("testloadSFClientConfigWithEnvVar failed");
    }
  }

  @Test
  public void testloadSFClientConfigWithDriverLoaction() {
    String configLocation =
        Paths.get(getConfigFilePathFromJDBCJarLocation(), SF_CLIENT_CONFIG_FILE_NAME).toString();
    Path configFilePath = Paths.get(configLocation);

    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());

      Files.delete(configFilePath);
    } catch (IOException e) {
      fail("testloadSFClientConfigWithClasspath failed");
    }
  }

  @Test
  public void testloadSFClientConfigWithUserHome() {
    String tmpDirectory = systemGetProperty("java.io.tmpdir");
    try (MockedStatic<SnowflakeUtil> mockedSnowflakeUtil = mockStatic(SnowflakeUtil.class)) {
      // mocking this as Jenkins/GH Action doesn't have write permissions on user.home directory.
      mockedSnowflakeUtil.when(() -> systemGetProperty("user.home")).thenReturn(tmpDirectory);

      Path configFilePath = Paths.get(systemGetProperty("user.home"), SF_CLIENT_CONFIG_FILE_NAME);
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());

      Files.delete(configFilePath);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      fail("testloadSFClientConfigWithUserHome failed: " + e.getMessage());
    }
  }

  @Test
  public void testloadSFClientNoConditionsMatch() throws IOException {
    SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
    assertTrue(actualConfig == null);
  }

  @Test
  public void testgetConfigFileNameFromJDBCJarLocation() {
    String jdbcDirectoryPath = getConfigFilePathFromJDBCJarLocation();
    assertTrue(jdbcDirectoryPath != null && !jdbcDirectoryPath.isEmpty());
  }

  @Test
  public void testgetConfigFileNameFromJDBCJarLocationForWindows() {
    try (MockedStatic<SnowflakeUtil> mockedSnowflakeUtil = mockStatic(SnowflakeUtil.class)) {
      mockedSnowflakeUtil.when(() -> systemGetProperty("os.name")).thenReturn("windows");
      String jdbcDirectoryPath = getConfigFilePathFromJDBCJarLocation();
      assertFalse(jdbcDirectoryPath.contains("/")); // windows use \ in paths
    }
  }
}
