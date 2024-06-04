package net.snowflake.client.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.Before;
import org.junit.Test;

public class SFConnectionConfigParserTest {

  private Path tempPath = null;
  private TomlMapper tomlMapper = new TomlMapper();

  @Before
  public void setUp() throws IOException {
    tempPath = Files.createTempDirectory(".snowflake");
    prepareConnectionConfigurationTomlFile();
  }

  @Test
  public void testLoadSFConnectionConfigWrongConfigurationName() {
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_HOME", tempPath.toString());
    assertThrows(
        SnowflakeSQLException.class,
        () -> SFConnectionConfigParser.buildConnectionParameters("unknown"));
  }

  @Test
  public void testLoadSFConnectionConfigInValidPath() {
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_HOME", Paths.get("unknownPath").toString());
    assertThrows(
        SnowflakeSQLException.class,
        () -> SFConnectionConfigParser.buildConnectionParameters("default"));
  }

  @Test
  public void testLoadSFConnectionConfigWithTokenFromFile()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_HOME", tempPath.toString());
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()));

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("default");
    assertNotNull(data);
    assertEquals("token_from_file", data.getParams().get("token"));
  }

  private void prepareConnectionConfigurationTomlFile(Map moreParameters) throws IOException {
    File file = new File(Paths.get(tempPath.toString(), "connections.toml").toUri());

    Map configuration = new HashMap();
    Map configurationParams = new HashMap();
    configurationParams.put("account", "snowaccount.us-west-2.aws");
    configurationParams.put("user", "user1");
    configurationParams.put("token", "testToken");
    configurationParams.put("port", "443");

    if (moreParameters != null) {
      moreParameters.forEach((k, v) -> configurationParams.put(k, v));
    }
    configuration.put("default", configurationParams);
    tomlMapper.writeValue(file, configuration);

    if (configurationParams.containsKey("token_file_path")) {
      FileWriter writer = new FileWriter(configurationParams.get("token_file_path").toString());
      writer.write("token_from_file");
      writer.close();
    }
  }

  private void prepareConnectionConfigurationTomlFile() throws IOException {
    prepareConnectionConfigurationTomlFile(null);
  }
}
