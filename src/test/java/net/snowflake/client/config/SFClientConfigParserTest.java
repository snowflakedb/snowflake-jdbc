package net.snowflake.client.config;

import static net.snowflake.client.config.SFClientConfigParser.SF_CLIENT_CONFIG_ENV_NAME;
import static net.snowflake.client.config.SFClientConfigParser.SF_CLIENT_CONFIG_FILE_NAME;
import static net.snowflake.client.config.SFClientConfigParser.convertToWindowsPath;
import static net.snowflake.client.config.SFClientConfigParser.getConfigFilePathFromJDBCJarLocation;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemSetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemUnsetEnv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class SFClientConfigParserTest {
  private static final String CONFIG_JSON =
      "{\"common\":{\"log_level\":\"info\",\"log_path\":\"/jdbc.log\"}}";
  private static final String CONFIG_JSON_WITH_UNKNOWN_PROPS =
      "{\"common\":{\"log_level\":\"info\",\"log_path\":\"/jdbc.log\",\"unknown_inside\":\"/unknown\"},\"unknown_outside\":\"/unknown\"}";

  private Path configFilePath;

  @AfterEach
  public void cleanup() throws IOException {
    if (configFilePath != null) {
      Files.deleteIfExists(configFilePath);
    }

    systemUnsetEnv(SF_CLIENT_CONFIG_ENV_NAME);
  }

  @Test
  public void testLoadSFClientConfigValidPath() throws IOException {
    configFilePath = Paths.get("config.json");
    Files.write(configFilePath, CONFIG_JSON.getBytes());
    SFClientConfig actualConfig =
        SFClientConfigParser.loadSFClientConfig(configFilePath.toString());
    assertEquals("info", actualConfig.getCommonProps().getLogLevel());
    assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());
    assertEquals("config.json", actualConfig.getConfigFilePath());
  }

  @Test
  public void testLoadSFClientConfigValidPathWithUnknownProperties() throws IOException {
    configFilePath = Paths.get("config.json");
    Files.write(configFilePath, CONFIG_JSON_WITH_UNKNOWN_PROPS.getBytes());
    SFClientConfig actualConfig =
        SFClientConfigParser.loadSFClientConfig(configFilePath.toString());
    assertEquals("info", actualConfig.getCommonProps().getLogLevel());
    assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());
  }

  @Test
  public void testLoadSFClientConfigInValidPath() {
    String configFilePath = "InvalidPath";
    assertThrows(IOException.class, () -> SFClientConfigParser.loadSFClientConfig(configFilePath));
  }

  @Test
  public void testLoadSFClientConfigInValidJson() {
    assertThrows(
        IOException.class,
        () -> {
          String invalidJson = "invalidJson";
          configFilePath = Paths.get("config.json");
          Files.write(configFilePath, invalidJson.getBytes());
          SFClientConfigParser.loadSFClientConfig(configFilePath.toString());
        });
  }

  @Test
  public void testLoadSFClientConfigWithEnvVar() throws IOException {
    configFilePath = Paths.get("config.json");
    Files.write(configFilePath, CONFIG_JSON.getBytes());
    systemSetEnv(SF_CLIENT_CONFIG_ENV_NAME, "config.json");
    SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
    assertEquals("info", actualConfig.getCommonProps().getLogLevel());
    assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());
  }

  @Test
  public void testLoadSFClientConfigWithDriverLocation() throws IOException {
    String configLocation =
        Paths.get(getConfigFilePathFromJDBCJarLocation(), SF_CLIENT_CONFIG_FILE_NAME).toString();
    configFilePath = Paths.get(configLocation);
    Files.write(configFilePath, CONFIG_JSON.getBytes());
    SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
    assertEquals("info", actualConfig.getCommonProps().getLogLevel());
    assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());
  }

  @Test
  public void testLoadSFClientConfigWithUserHome() throws IOException {
    String tmpDirectory = systemGetProperty("java.io.tmpdir");
    try (MockedStatic<SnowflakeUtil> mockedSnowflakeUtil = mockStatic(SnowflakeUtil.class)) {
      // mocking this as Jenkins/GH Action doesn't have write permissions on user.home directory.
      mockedSnowflakeUtil.when(() -> systemGetProperty("user.home")).thenReturn(tmpDirectory);

      configFilePath = Paths.get(systemGetProperty("user.home"), SF_CLIENT_CONFIG_FILE_NAME);
      Files.write(configFilePath, CONFIG_JSON.getBytes());
      SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
      assertEquals("info", actualConfig.getCommonProps().getLogLevel());
      assertEquals("/jdbc.log", actualConfig.getCommonProps().getLogPath());
    }
  }

  @Test
  public void testLoadSFClientNoConditionsMatch() throws IOException {
    SFClientConfig actualConfig = SFClientConfigParser.loadSFClientConfig(null);
    assertNull(actualConfig);
  }

  @Test
  public void testGetConfigFileNameFromJDBCJarLocation() {
    String jdbcDirectoryPath = getConfigFilePathFromJDBCJarLocation();
    assertTrue(jdbcDirectoryPath != null && !jdbcDirectoryPath.isEmpty());
  }

  @Test
  public void testConvertToWindowsPath() {
    String mockWindowsPath = "C:/Program Files/example.txt";
    String resultWindowsPath = "C:\\Program Files\\example.txt";
    String[] testCases = new String[] {"", "file:\\", "\\\\", "/", "nested:\\"};
    String mockCloudPrefix = "cloud://";

    for (String testcase : testCases) {
      assertEquals(resultWindowsPath, convertToWindowsPath(testcase + mockWindowsPath));
    }

    assertEquals(
        mockCloudPrefix + resultWindowsPath,
        convertToWindowsPath(mockCloudPrefix + mockWindowsPath));
  }

  @Test
  void testSFClientConfigConstructorAndAccessors() {
    SFClientConfig.CommonProps props = new SFClientConfig.CommonProps();
    props.setLogLevel("DEBUG");
    props.setLogPath("/tmp/logs");
    SFClientConfig config = new SFClientConfig(props);
    config.setConfigFilePath("/etc/snowflake/config.json");

    assertEquals(props, config.getCommonProps());
    assertEquals("/etc/snowflake/config.json", config.getConfigFilePath());
  }

  @Test
  void testCommonPropsConstructorAndAccessors() {
    SFClientConfig.CommonProps props = new SFClientConfig.CommonProps();
    props.setLogLevel("DEBUG");
    props.setLogPath("/var/logs/snowflake.log");

    assertEquals("DEBUG", props.getLogLevel());
    assertEquals("/var/logs/snowflake.log", props.getLogPath());
  }

  @Test
  void testSFClientConfigEqualsAndHashCode() {
    SFClientConfig.CommonProps props1 = new SFClientConfig.CommonProps();
    props1.setLogLevel("INFO");
    props1.setLogPath("/tmp");

    SFClientConfig.CommonProps props2 = new SFClientConfig.CommonProps();
    props2.setLogLevel("INFO");
    props2.setLogPath("/tmp");
    SFClientConfig config1 = new SFClientConfig(props1);
    SFClientConfig config2 = new SFClientConfig(props2);

    assertEquals(config1, config2);
    assertEquals(config1.hashCode(), config2.hashCode());

    // Negative test
    props2.setLogLevel("DEBUG");
    assertNotEquals(config1, new SFClientConfig(props2));
  }

  @Test
  void testCommonPropsEqualsAndHashCode() {
    SFClientConfig.CommonProps props1 = new SFClientConfig.CommonProps();
    props1.setLogLevel("WARN");
    props1.setLogPath("/opt/logs");
    SFClientConfig.CommonProps props2 = new SFClientConfig.CommonProps();
    props2.setLogLevel("WARN");
    props2.setLogPath("/opt/logs");

    assertEquals(props1, props2);
    assertEquals(props1.hashCode(), props2.hashCode());

    // Negative test
    props2.setLogLevel("ERROR");
    assertNotEquals(props1, props2);
  }
}
