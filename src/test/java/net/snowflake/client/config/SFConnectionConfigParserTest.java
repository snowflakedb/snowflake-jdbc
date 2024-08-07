package net.snowflake.client.config;

import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_HOME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeFalse;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.RunningNotOnLinuxMac;
import net.snowflake.client.core.Constants;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SFConnectionConfigParserTest {

  private Path tempPath = null;
  private TomlMapper tomlMapper = new TomlMapper();

  @Before
  public void setUp() throws IOException {
    tempPath = Files.createTempDirectory(".snowflake");
  }

  @After
  public void close() throws IOException {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_HOME_KEY);
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY);
    Files.walk(tempPath).map(Path::toFile).forEach(File::delete);
    Files.delete(tempPath);
  }

  @Test
  public void testLoadSFConnectionConfigWrongConfigurationName()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "unknown");
    prepareConnectionConfigurationTomlFile();
    ConnectionParameters connectionParameters =
        SFConnectionConfigParser.buildConnectionParameters();
    assertNull(connectionParameters);
  }

  @Test
  public void testLoadSFConnectionConfigInValidPath() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, Paths.get("unknownPath").toString());
    prepareConnectionConfigurationTomlFile();
    assertNull(SFConnectionConfigParser.buildConnectionParameters());
  }

  @Test
  public void testLoadSFConnectionConfigWithTokenFromFile()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()));

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters();
    assertNotNull(data);
    assertEquals(tokenFile.toString(), data.getParams().get("token_file_path"));
  }

  @Test
  public void testThrowErrorWhenWrongPermissionsForConnectionConfigurationFile()
      throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), false, false);
    assumeFalse(RunningNotOnLinuxMac.isNotRunningOnLinuxMac());
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters());
  }

  @Test
  public void testThrowErrorWhenWrongPermissionsForTokenFile() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), true, false);
    assumeFalse(RunningNotOnLinuxMac.isNotRunningOnLinuxMac());
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters());
  }

  @Test
  public void testLoadSFConnectionConfigWithHostConfigured()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap();
    extraparams.put("host", "snowflake.reg.local");
    extraparams.put("account", null);
    extraparams.put("port", "8082");
    extraparams.put("token", "testToken");
    prepareConnectionConfigurationTomlFile(extraparams);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters();
    assertNotNull(data);
    assertEquals("jdbc:snowflake://snowflake.reg.local:8082", data.getUrl());
    assertEquals("oauth", data.getParams().get("authenticator"));
    assertEquals("testToken", data.getParams().get("token"));
  }

  @Test
  public void shouldThrowExceptionIfNoneOfHostAndAccountIsSet() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap();
    extraparams.put("host", null);
    extraparams.put("account", null);
    prepareConnectionConfigurationTomlFile(extraparams);
    Assert.assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters());
  }

  private void prepareConnectionConfigurationTomlFile() throws IOException {
    prepareConnectionConfigurationTomlFile(null, true, true);
  }

  private void prepareConnectionConfigurationTomlFile(Map moreParameters) throws IOException {
    prepareConnectionConfigurationTomlFile(moreParameters, true, true);
  }

  private void prepareConnectionConfigurationTomlFile(
      Map moreParameters, boolean onlyUserPermissionConnection, boolean onlyUserPermissionToken)
      throws IOException {
    Path path = Paths.get(tempPath.toString(), "connections.toml");
    Path filePath = createFilePathWithPermission(path, onlyUserPermissionConnection);
    File file = filePath.toFile();

    Map configuration = new HashMap();
    Map configurationParams = new HashMap();
    configurationParams.put("account", "snowaccount.us-west-2.aws");
    configurationParams.put("user", "user1");
    configurationParams.put("port", "443");
    configurationParams.put("authenticator", "oauth");

    if (moreParameters != null) {
      moreParameters.forEach((k, v) -> configurationParams.put(k, v));
    }
    configuration.put("default", configurationParams);
    tomlMapper.writeValue(file, configuration);

    if (configurationParams.containsKey("token_file_path")) {
      Path tokenFilePath =
          createFilePathWithPermission(
              Paths.get(configurationParams.get("token_file_path").toString()),
              onlyUserPermissionToken);
      Files.write(tokenFilePath, "token_from_file".getBytes());
    }
  }

  private Path createFilePathWithPermission(Path path, boolean onlyUserPermission)
      throws IOException {
    if (Constants.getOS() != Constants.OS.WINDOWS) {
      FileAttribute<Set<PosixFilePermission>> fileAttribute =
          onlyUserPermission
              ? PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
              : PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrw----"));
      return Files.createFile(path, fileAttribute);
    } else {
      return Files.createFile(path);
    }
  }
}
