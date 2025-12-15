package net.snowflake.client.jdbc;

import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_HOME_KEY;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(TestTags.CONNECTION)
public class AutoConnectionConfigurationLatestIT extends BaseJDBCTest {
  private static final List<String> ENV_VARIABLES_KEYS =
      new ArrayList<>(Arrays.asList(SNOWFLAKE_HOME_KEY, SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY));
  private Path tempPath = null;
  private TomlMapper tomlMapper = new TomlMapper();
  private Map<String, String> envVariables = new HashMap<String, String>();
  Map<String, String> params = new HashMap<String, String>();

  @BeforeEach
  public void setUp() throws IOException {
    params = BaseJDBCTest.getConnectionParameters();
    tempPath = Files.createTempDirectory(".snowflake");
    ENV_VARIABLES_KEYS.forEach(
        key -> {
          if (SnowflakeUtil.systemGetEnv(key) != null) {
            envVariables.put(key, SnowflakeUtil.systemGetEnv(key));
          }
        });
  }

  @AfterEach
  public void cleanUp() throws IOException {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_HOME_KEY);
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY);
    Files.walk(tempPath).map(Path::toFile).forEach(File::delete);
    Files.delete(tempPath);
    envVariables.forEach(SnowflakeUtil::systemSetEnv);
  }

  @ParameterizedTest
  @MethodSource("connectionScenarios")
  public void testConnectionScenarios(String connectionName, boolean shouldThrow)
      throws IOException, SQLException {
    prepareConnectionConfigurationTomlFile();
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    final String connectionString;

    if (null != connectionName && connectionName.equals("systemEnvConfig")) {
      SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, connectionName);
      connectionString = SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX;
    } else {
      connectionString =
          connectionName != null
              ? SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX + "?connectionName=" + connectionName
              : SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX;
    }

    if (shouldThrow) {
      SnowflakeSQLException ex =
          assertThrows(
              SnowflakeSQLException.class,
              () -> DriverManager.getConnection(connectionString, null));
      assertTrue(ex.getMessage().contains("not found in connections.toml file."));
    } else {
      try (Connection con = DriverManager.getConnection(connectionString, null)) {
        assertNotNull(con);
      } catch (Exception e) {
        fail("Should not fail. " + e.getMessage());
      }
    }
  }

  private void prepareConnectionConfigurationTomlFile() throws IOException {
    Path path = Paths.get(tempPath.toString(), "connections.toml");
    Path filePath = createFilePathWithPermission(path);
    File file = filePath.toFile();

    Map<String, Map<String, String>> configuration = new HashMap<String, Map<String, String>>();
    Map<String, String> configurationParams = new HashMap<String, String>();
    configurationParams.put("account", params.get("account"));
    configurationParams.put("user", params.get("user"));
    configurationParams.put("port", params.get("port"));
    configurationParams.put("host", params.get("host"));
    configurationParams.put("ssl", params.get("ssl"));

    if (!Strings.isNullOrEmpty(params.get("private_key_file"))) {
      configurationParams.put("private_key_file", params.get("private_key_file"));
      configurationParams.put("authenticator", params.get("authenticator"));
      if (!Strings.isNullOrEmpty(params.get("private_key_pwd"))) {
        configurationParams.put("private_key_pwd", params.get("private_key_pwd"));
      }
    } else if (!Strings.isNullOrEmpty(params.get("password"))) {
      configurationParams.put("password", params.get("password"));
    }

    configuration.put("default", configurationParams);
    configuration.put("readOnly", configurationParams);
    configuration.put("systemEnvConfig", configurationParams);
    tomlMapper.writeValue(file, configuration);
  }

  private Path createFilePathWithPermission(Path path) throws IOException {
    if (!isWindows()) {
      FileAttribute<Set<PosixFilePermission>> fileAttribute =
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
      return Files.createFile(path, fileAttribute);
    } else {
      return Files.createFile(path);
    }
  }

  private static List<Arguments> connectionScenarios() {
    return Arrays.asList(
        Arguments.of("notConfigured", true),
        Arguments.of("readOnly", false),
        Arguments.of("systemEnvConfig", false),
        Arguments.of(null, false));
  }
}
