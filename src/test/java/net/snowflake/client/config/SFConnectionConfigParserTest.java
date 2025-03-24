package net.snowflake.client.config;

import static net.snowflake.client.AssumptionUtils.assumeRunningOnLinuxMac;
import static net.snowflake.client.config.SFConnectionConfigParser.SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION;
import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_HOME_KEY;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SFConnectionConfigParserTest {

  private static final List<String> ENV_VARIABLES_KEYS =
      new ArrayList<>(
          Arrays.asList(
              SNOWFLAKE_HOME_KEY,
              SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY,
              SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION));
  private Path tempPath = null;
  private TomlMapper tomlMapper = new TomlMapper();
  private Map<String, String> envVariables = new HashMap();

  @BeforeEach
  public void setUp() throws IOException {
    tempPath = Files.createTempDirectory(".snowflake");
    ENV_VARIABLES_KEYS.stream()
        .forEach(
            key -> {
              if (SnowflakeUtil.systemGetEnv(key) != null) {
                envVariables.put(key, SnowflakeUtil.systemGetEnv(key));
              }
            });
  }

  @AfterEach
  public void close() throws IOException {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_HOME_KEY);
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY);
    SnowflakeUtil.systemUnsetEnv(SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION);
    Files.walk(tempPath).map(Path::toFile).forEach(File::delete);
    Files.delete(tempPath);
    envVariables.forEach((key, value) -> SnowflakeUtil.systemSetEnv(key, value));
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
    assumeRunningOnLinuxMac();
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters());
  }

  @Test
  public void testThrowErrorWhenWrongPermissionsForTokenFile() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), true, false);
    assumeRunningOnLinuxMac();
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters());
  }

  @Test
  public void testNoThrowErrorWhenWrongPermissionsForTokenFileButSkippingFlagIsEnabled()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    SnowflakeUtil.systemSetEnv(SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION, "true");
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), true, false);

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters();
    assertNotNull(data);
    assertEquals(tokenFile.toString(), data.getParams().get("token_file_path"));
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
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters());
  }

  @Test
  public void shouldThrowExceptionIfTokenIsNotSetForOauth() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    SnowflakeUtil.systemSetEnv(SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION, "true");
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), true, false, "");

    assertThrows(
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
    prepareConnectionConfigurationTomlFile(
        moreParameters, onlyUserPermissionConnection, onlyUserPermissionToken, "token_from_file");
  }

  private void prepareConnectionConfigurationTomlFile(
      Map moreParameters,
      boolean onlyUserPermissionConnection,
      boolean onlyUserPermissionToken,
      String token)
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
      Files.write(tokenFilePath, token.getBytes());
      Path emptyTokenFilePath =
          createFilePathWithPermission(
              Paths.get(
                  configurationParams
                      .get("token_file_path")
                      .toString()
                      .replaceAll("token", "emptytoken")),
              onlyUserPermissionToken);
      Files.write(emptyTokenFilePath, "".getBytes());
    }
  }

  private Path createFilePathWithPermission(Path path, boolean onlyUserPermission)
      throws IOException {
    if (!isWindows()) {
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
