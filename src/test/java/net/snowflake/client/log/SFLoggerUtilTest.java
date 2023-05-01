package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.*;
import static net.snowflake.client.log.SFLoggerUtil.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

public class SFLoggerUtilTest {
  private static final String CONFIG_JSON =
      "{\"common\":{\"log_level\":\"info\",\"log_path\":\"/jdbc.log\"}}";

  @Test
  public void testloadSFClientConfigValidPath() {
    Path configFilePath = Paths.get("config.json");
    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig = SFLoggerUtil.loadSFClientConfig(configFilePath.toString());
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
    SFClientConfig config = SFLoggerUtil.loadSFClientConfig(configFilePath.toString());
    assertTrue(config == null);
  }

  @Test
  public void testloadSFClientConfigInValidJson() {
    try {
      String invalidJson = "invalidJson";
      Path configFilePath = Paths.get("config.json");
      Files.write(configFilePath, invalidJson.getBytes());
      SFClientConfig config = SFLoggerUtil.loadSFClientConfig(configFilePath.toString());

      assertTrue(config == null);
    } catch (IOException e) {
      fail("testloadSFClientConfigInValidJson");
    }
  }

  @Test
  public void testloadSFClientConfigWithEnvVar() {
    Path configFilePath = Paths.get("config.json");

    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      systemSetEnv(SF_CLIENT_CONFIG_ENV_NAME, "config.json");
      SFClientConfig actualConfig = SFLoggerUtil.loadSFClientConfig(null);
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
    String configLocation = getConfigFilePathFromJDBCJarLocation();
    Path configFilePath = Paths.get(configLocation);

    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig = SFLoggerUtil.loadSFClientConfig(null);
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
      SFClientConfig actualConfig = SFLoggerUtil.loadSFClientConfig(null);
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());

      Files.delete(configFilePath);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      fail("testloadSFClientConfigWithUserHome failed: " + e.getMessage());
    }
  }

  @Test
  public void testloadSFClientConfigWithTmpDirectory() {
    Path configFilePath =
        Paths.get(systemGetProperty("java.io.tmpdir"), SF_CLIENT_CONFIG_FILE_NAME);
    try {
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig = SFLoggerUtil.loadSFClientConfig(null);
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());

      Files.delete(configFilePath);
    } catch (IOException e) {
      fail("testloadSFClientConfigWithTmpDirectory failed");
    }
  }

  @Test
  public void testloadSFClientNoConditionsMatch() {
    SFClientConfig actualConfig = SFLoggerUtil.loadSFClientConfig(null);
    assertTrue(actualConfig == null);
  }

  @Test
  public void testgetConfigFileNameFromJDBCJarLocation() {
    String jdbcDirectoryPath = getConfigFilePathFromJDBCJarLocation();
    assertTrue(jdbcDirectoryPath != null && !jdbcDirectoryPath.isEmpty());
  }
}
