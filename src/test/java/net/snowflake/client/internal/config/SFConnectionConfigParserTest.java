package net.snowflake.client.internal.config;

import static net.snowflake.client.AssumptionUtils.assumeRunningOnLinuxMac;
import static net.snowflake.client.internal.config.SFConnectionConfigParser.SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION;
import static net.snowflake.client.internal.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static net.snowflake.client.internal.config.SFConnectionConfigParser.SNOWFLAKE_HOME_KEY;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isWindows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.driver.AutoConfigurationHelper;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
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
  private Map<String, String> envVariables = new HashMap<>();

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
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters(""));
  }

  @Test
  public void testLoadSFConnectionConfigInValidPath() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, Paths.get("unknownPath").toString());
    prepareConnectionConfigurationTomlFile();
    assertNull(SFConnectionConfigParser.buildConnectionParameters(""));
  }

  @Test
  public void testLoadSFConnectionConfigWithTokenFromFile()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()));

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
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
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters(""));
  }

  @Test
  public void testThrowErrorWhenWrongPermissionsForTokenFile() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), true, false);
    assumeRunningOnLinuxMac();
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters(""));
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

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals(tokenFile.toString(), data.getParams().get("token_file_path"));
  }

  @Test
  public void testNoThrowErrorWhenWrongPermissionsForConnectionConfigButSkippingFlagIsEnabled()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    SnowflakeUtil.systemSetEnv(SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION, "true");
    File tokenFile = new File(Paths.get(tempPath.toString(), "token").toUri());
    prepareConnectionConfigurationTomlFile(
        Collections.singletonMap("token_file_path", tokenFile.toString()), false, false);
    assumeRunningOnLinuxMac();

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals(tokenFile.toString(), data.getParams().get("token_file_path"));
  }

  @Test
  public void testLoadSFConnectionConfigWithHostConfigured()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap<>();
    extraparams.put("host", "snowflake.reg.local");
    extraparams.put("account", null);
    extraparams.put("port", "8082");
    extraparams.put("token", "testToken");
    prepareConnectionConfigurationTomlFile(extraparams);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://snowflake.reg.local:8082", data.getUrl());
    assertEquals("oauth", data.getParams().get("authenticator"));
    assertEquals("testToken", data.getParams().get("token"));
  }

  @Test
  public void testDefaultPortIs443WhenNeitherPortNorProtocolIsSet()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, null);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:443", data.getUrl());
  }

  @Test
  public void testDefaultPortIs443WhenProtocolIsHttps() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, "https");
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:443", data.getUrl());
  }

  @Test
  public void testDefaultPortIs80WhenProtocolIsHttp() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, "http");
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals(
        "jdbc:snowflake://http://myorg-myaccount.snowflakecomputing.com:80", data.getUrl());
  }

  @Test
  public void testExplicitPortIsPreservedRegardlessOfProtocol()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol("8082", "http");
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals(
        "jdbc:snowflake://http://myorg-myaccount.snowflakecomputing.com:8082", data.getUrl());
  }

  @Test
  public void testUrlParametersAreMergedIntoTomlConfiguration()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, null);
    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&tracing=ALL&disablePlatformDetection=true");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:443", data.getUrl());
    assertEquals("ALL", data.getParams().get("tracing"));
    assertEquals("true", data.getParams().get("disablePlatformDetection"));
    assertEquals("TOML_USER", data.getParams().get("user"));
    assertEquals("TOML_PASS", data.getParams().get("password"));
    assertEquals("TOML_WH", data.getParams().get("warehouse"));
  }

  @Test
  public void testUrlParameterOverridesTomlValueForSameKey()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol("8082", "http");
    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&port=443&protocol=https&warehouse=URL_WH&tracing=ALL");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:443", data.getUrl());
    assertEquals("443", data.getParams().get("port"));
    assertEquals("https", data.getParams().get("protocol"));
    assertEquals("URL_WH", data.getParams().get("warehouse"));
    assertEquals("ALL", data.getParams().get("tracing"));
    assertEquals("TOML_USER", data.getParams().get("user"));
    assertEquals("TOML_PASS", data.getParams().get("password"));
    assertEquals("myorg-myaccount", data.getParams().get("account"));
    Set<String> expectedKeys =
        new HashSet<>(
            Arrays.asList(
                "account", "user", "password", "warehouse", "port", "protocol", "tracing"));
    Set<String> actualKeys = data.getParams().stringPropertyNames();
    assertEquals(expectedKeys, actualKeys);
  }

  @Test
  public void testUrlParameterOverridesTomlUser() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, null);
    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&user=URL_USER");
    assertNotNull(data);
    assertEquals("URL_USER", data.getParams().get("user"));
  }

  @Test
  public void testUrlParametersWithNoExtraParamsKeepsTomlValues()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol("8082", "http");
    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default");
    assertNotNull(data);
    assertEquals(
        "jdbc:snowflake://http://myorg-myaccount.snowflakecomputing.com:8082", data.getUrl());
    assertEquals("TOML_USER", data.getParams().get("user"));
    assertEquals("TOML_PASS", data.getParams().get("password"));
    assertEquals("TOML_WH", data.getParams().get("warehouse"));
  }

  @Test
  public void testHttpProtocolFromTomlIsEmbeddedInUrl() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap<>();
    extraparams.put("host", "snowflake.reg.local");
    extraparams.put("account", null);
    extraparams.put("port", "8082");
    extraparams.put("token", "testToken");
    extraparams.put("protocol", "http");
    prepareConnectionConfigurationTomlFile(extraparams);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://http://snowflake.reg.local:8082", data.getUrl());
  }

  @Test
  public void testHttpsProtocolFromTomlProducesStandardUrl()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap<>();
    extraparams.put("host", "snowflake.reg.local");
    extraparams.put("account", null);
    extraparams.put("port", "8082");
    extraparams.put("token", "testToken");
    extraparams.put("protocol", "https");
    prepareConnectionConfigurationTomlFile(extraparams);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://snowflake.reg.local:8082", data.getUrl());
  }

  @Test
  public void testDefaultPortIs443WhenProtocolIsEmptyString()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap<>();
    extraparams.put("host", "snowflake.reg.local");
    extraparams.put("account", null);
    extraparams.put("port", null);
    extraparams.put("token", "testToken");
    extraparams.put("protocol", "");
    prepareConnectionConfigurationTomlFile(extraparams);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://snowflake.reg.local:443", data.getUrl());
  }

  @Test
  public void shouldThrowExceptionIfNoneOfHostAndAccountIsSet() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    Map<String, String> extraparams = new HashMap<>();
    extraparams.put("host", null);
    extraparams.put("account", null);
    prepareConnectionConfigurationTomlFile(extraparams);
    assertThrows(
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters(""));
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
        SnowflakeSQLException.class, () -> SFConnectionConfigParser.buildConnectionParameters(""));
  }

  // URL "db" and TOML "database" resolve to same property; URL wins, no duplicate error
  @Test
  public void testUrlDbAliasDoesNotConflictWithTomlDatabase()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB");

    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&db=URL_DB&tracing=WARNING");
    assertNotNull(data);
    assertEquals("URL_DB", data.getParams().get("db"));
    assertNull(data.getParams().get("database"));
    assertEquals("WARNING", data.getParams().get("tracing"));
    assertEquals("TOML_USER", data.getParams().get("user"));
  }

  // URL "db" + other overrides vs TOML "database" + extras; URL wins on conflicts, TOML retained
  // otherwise
  @Test
  public void testUrlDbAliasOverridesTomlDatabaseWithOtherConflicts()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB", "TOML_WH", "TOML_ROLE");

    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&db=URL_DB&warehouse=URL_WH&tracing=ALL");
    assertNotNull(data);
    assertEquals("URL_DB", data.getParams().get("db"));
    assertNull(data.getParams().get("database"));
    assertEquals("URL_WH", data.getParams().get("warehouse"));
    assertEquals("ALL", data.getParams().get("tracing"));
    assertEquals("TOML_ROLE", data.getParams().get("role"));
    assertEquals("TOML_USER", data.getParams().get("user"));
  }

  // TOML has a non-SFSessionProperty key (CLIENT_RESULT_CHUNK_SIZE); it survives the alias-aware
  // merge
  @Test
  public void testNonSFSessionPropertyFromTomlIsPreservedAfterMerge()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithCustomSessionParam("TOML_DB", "10");

    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&db=URL_DB");
    assertNotNull(data);
    assertEquals("URL_DB", data.getParams().get("db"));
    assertNull(data.getParams().get("database"));
    assertEquals("10", data.getParams().get("CLIENT_RESULT_CHUNK_SIZE"));
    assertEquals("TOML_USER", data.getParams().get("user"));
  }

  // Properties + URL + TOML with no overlapping keys; all values correctly merged
  @Test
  public void testPropertiesAndUrlNoConflict() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, null);

    Properties info = new Properties();
    info.setProperty("schema", "PROP_SCHEMA");
    info.setProperty("role", "PROP_ROLE");

    ConnectionParameters data =
        AutoConfigurationHelper.resolveConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&db=URL_DB&tracing=WARNING", info);
    assertNotNull(data);
    assertEquals("URL_DB", data.getParams().get("db"));
    assertEquals("WARNING", data.getParams().get("tracing"));
    assertEquals("PROP_SCHEMA", data.getParams().get("schema"));
    assertEquals("PROP_ROLE", data.getParams().get("role"));
    assertEquals("TOML_USER", data.getParams().get("user"));
    assertEquals("TOML_PASS", data.getParams().get("password"));
    assertEquals("TOML_WH", data.getParams().get("warehouse"));
  }

  // Properties + URL + TOML all use distinct keys; TOML "database" preserved when no alias conflict
  @Test
  public void testPropertiesUrlAndTomlNoConflict() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB");

    Properties info = new Properties();
    info.setProperty("schema", "PROP_SCHEMA");

    ConnectionParameters data =
        AutoConfigurationHelper.resolveConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&tracing=WARNING", info);
    assertNotNull(data);
    assertEquals("TOML_DB", data.getParams().get("database"));
    assertEquals("WARNING", data.getParams().get("tracing"));
    assertEquals("PROP_SCHEMA", data.getParams().get("schema"));
    assertEquals("TOML_USER", data.getParams().get("user"));
  }

  // Properties "warehouse" overrides both URL and TOML; non-conflicting TOML/URL values preserved
  @Test
  public void testPropertiesOverridesUrlAndTomlOnConflict()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB", "TOML_WH", "TOML_ROLE");

    Properties info = new Properties();
    info.setProperty("warehouse", "PROP_WH");
    info.setProperty("schema", "PROP_SCHEMA");

    ConnectionParameters data =
        AutoConfigurationHelper.resolveConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&warehouse=URL_WH&tracing=ALL", info);
    assertNotNull(data);
    assertEquals("PROP_WH", data.getParams().get("warehouse"));
    assertEquals("PROP_SCHEMA", data.getParams().get("schema"));
    assertEquals("ALL", data.getParams().get("tracing"));
    assertEquals("TOML_ROLE", data.getParams().get("role"));
    assertEquals("TOML_USER", data.getParams().get("user"));
  }

  // Three-way alias conflict: TOML "database", URL "db", Properties "db"; Properties wins, no
  // duplicate
  @Test
  public void testPropertiesDbOverridesUrlDbAndTomlDatabase()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB", "TOML_WH", "TOML_ROLE");

    Properties info = new Properties();
    info.setProperty("db", "PROP_DB");
    info.setProperty("schema", "PROP_SCHEMA");
    info.setProperty("queryTimeout", "30");

    ConnectionParameters data =
        AutoConfigurationHelper.resolveConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default"
                + "&db=URL_DB&tracing=WARNING&loginTimeout=120",
            info);
    assertNotNull(data);
    // Properties win for db (alias conflict resolved: TOML's "database" removed by URL's "db",
    // then Properties' "db" overwrites URL's "db")
    assertEquals("PROP_DB", data.getParams().get("db"));
    assertNull(data.getParams().get("database"));
    // Properties' own values
    assertEquals("PROP_SCHEMA", data.getParams().get("schema"));
    assertEquals("30", data.getParams().get("queryTimeout"));
    // URL values not overridden by Properties
    assertEquals("WARNING", data.getParams().get("tracing"));
    assertEquals("120", data.getParams().get("loginTimeout"));
    // TOML values not overridden
    assertEquals("TOML_WH", data.getParams().get("warehouse"));
    assertEquals("TOML_ROLE", data.getParams().get("role"));
    assertEquals("TOML_USER", data.getParams().get("user"));
    // Verify no duplicate keys that could cause "property specified more than once"
    Set<String> keys = data.getParams().stringPropertyNames();
    assertFalse(keys.contains("database"));
    assertTrue(keys.contains("db"));
  }

  // URL with both "db" and "database" (same-source alias duplicates) must throw
  @Test
  public void testUrlSameSourceAliasDuplicateThrows() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, null);

    assertThrows(
        SnowflakeSQLException.class,
        () ->
            SFConnectionConfigParser.buildConnectionParameters(
                "jdbc:snowflake:auto?connectionName=default&db=URL_DB_A&database=URL_DB_B"));
  }

  // URL alias duplicates still throw even when TOML also has the same property
  @Test
  public void testUrlSameSourceAliasDuplicateThrowsEvenWithTomlPresent() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB_X");

    assertThrows(
        SnowflakeSQLException.class,
        () ->
            SFConnectionConfigParser.buildConnectionParameters(
                "jdbc:snowflake:auto?connectionName=default&db=URL_DB_A&database=URL_DB_B"));
  }

  // URL with case-variant duplicates ("database" and "DATABASE") must throw
  @Test
  public void testUrlCaseVariantDuplicateThrows() throws IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithPortAndProtocol(null, null);

    assertThrows(
        SnowflakeSQLException.class,
        () ->
            SFConnectionConfigParser.buildConnectionParameters(
                "jdbc:snowflake:auto?connectionName=default&database=URL_DB_A&DATABASE=URL_DB_B"));
  }

  // Cross-source case-insensitive override: TOML "database" overridden by URL "DATABASE"
  @Test
  public void testCrossSourceCaseInsensitiveOverride() throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB_X");

    ConnectionParameters data =
        SFConnectionConfigParser.buildConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&DATABASE=URL_DB_A");
    assertNotNull(data);
    assertEquals("URL_DB_A", data.getParams().get("DATABASE"));
    // TOML's "database" key must be removed when URL's "DATABASE" overrides it
    assertNull(data.getParams().get("database"));
  }

  // Non-String PrivateKey in Properties is preserved through the three-way merge
  @Test
  public void testNonStringPrivateKeyPreservedInThreeWayMerge() throws Exception {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB_X");

    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    PrivateKey mockKey = keyGen.generateKeyPair().getPrivate();

    Properties info = new Properties();
    info.put("privateKey", mockKey);
    info.setProperty("db", "PROP_DB_A");

    ConnectionParameters data =
        AutoConfigurationHelper.resolveConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&tracing=WARNING", info);
    assertNotNull(data);
    assertSame(mockKey, data.getParams().get("privateKey"));
    assertEquals("PROP_DB_A", data.getParams().get("db"));
    assertNull(data.getParams().get("database"));
    assertEquals("WARNING", data.getParams().get("tracing"));
    assertEquals("TOML_USER", data.getParams().get("user"));
  }

  // Verifies deferred log messages are populated and provenance is sorted alphabetically
  @Test
  public void testDeferredLogMessagesAndSortedProvenance()
      throws SnowflakeSQLException, IOException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
    prepareTomlWithDatabase("TOML_DB", "TOML_WH", "TOML_ROLE");

    Properties info = new Properties();
    info.setProperty("warehouse", "PROP_WH");
    info.setProperty("schema", "PROP_SCHEMA");

    ConnectionParameters data =
        AutoConfigurationHelper.resolveConnectionParameters(
            "jdbc:snowflake:auto?connectionName=default&db=URL_DB&tracing=WARNING", info);
    assertNotNull(data);

    @SuppressWarnings("unchecked")
    List<String> deferred =
        (List<String>) data.getParams().get(AutoConfigurationHelper.DEFERRED_LOG_MESSAGES_KEY);
    assertNotNull(deferred);
    assertFalse(deferred.isEmpty());

    assertTrue(deferred.stream().anyMatch(m -> m.contains("Attempting to load the configuration")));
    assertTrue(
        deferred.stream().anyMatch(m -> m.contains("Reading connection parameters from file")));
    assertTrue(deferred.stream().anyMatch(m -> m.contains("found in connections.toml")));
    assertTrue(
        deferred.stream()
            .anyMatch(m -> m.contains("Url created using parameters from connection")));
    assertTrue(deferred.stream().anyMatch(m -> m.contains("Autoconfiguration is enabled")));

    String provenanceLine =
        deferred.stream()
            .filter(m -> m.contains("Auto-configuration resolved properties"))
            .findFirst()
            .orElse(null);
    assertNotNull(provenanceLine);

    // Provenance must be sorted alphabetically by key
    int accountIdx = provenanceLine.indexOf("account(");
    int dbIdx = provenanceLine.indexOf("db(");
    int passwordIdx = provenanceLine.indexOf("password(");
    int roleIdx = provenanceLine.indexOf("role(");
    int schemaIdx = provenanceLine.indexOf("schema(");
    int tracingIdx = provenanceLine.indexOf("tracing(");
    int userIdx = provenanceLine.indexOf("user(");
    int warehouseIdx = provenanceLine.indexOf("warehouse(");
    assertTrue(accountIdx < dbIdx);
    assertTrue(dbIdx < passwordIdx);
    assertTrue(passwordIdx < roleIdx);
    assertTrue(roleIdx < schemaIdx);
    assertTrue(schemaIdx < tracingIdx);
    assertTrue(tracingIdx < userIdx);
    assertTrue(userIdx < warehouseIdx);

    // Verify provenance content
    assertTrue(provenanceLine.contains("db(URL(overrode TOML))"));
    assertTrue(provenanceLine.contains("warehouse(PROP(overrode TOML))"));
    assertTrue(provenanceLine.contains("schema(PROP)"));
    assertTrue(provenanceLine.contains("tracing(URL)"));
    assertTrue(provenanceLine.contains("role(TOML)"));
    assertTrue(provenanceLine.contains("account(TOML)"));
  }

  private void prepareTomlWithDatabase(String databaseValue) throws IOException {
    prepareTomlWithDatabase(databaseValue, null, null, null);
  }

  private void prepareTomlWithDatabase(
      String databaseValue, String warehouseValue, String roleValue) throws IOException {
    prepareTomlWithDatabase(databaseValue, warehouseValue, roleValue, null);
  }

  private void prepareTomlWithDatabase(
      String databaseValue,
      String warehouseValue,
      String roleValue,
      Map<String, String> extraParams)
      throws IOException {
    Path path = Paths.get(tempPath.toString(), "connections.toml");
    Path filePath = createFilePathWithPermission(path, true);
    File file = filePath.toFile();

    Map<String, Object> configurationParams = new HashMap<>();
    configurationParams.put("account", "myorg-myaccount");
    configurationParams.put("user", "TOML_USER");
    configurationParams.put("password", "TOML_PASS");
    configurationParams.put("database", databaseValue);
    if (warehouseValue != null) {
      configurationParams.put("warehouse", warehouseValue);
    }
    if (roleValue != null) {
      configurationParams.put("role", roleValue);
    }
    if (extraParams != null) {
      configurationParams.putAll(extraParams);
    }

    Map<String, Object> configuration = new HashMap<>();
    configuration.put("default", configurationParams);
    tomlMapper.writeValue(file, configuration);
  }

  private void prepareTomlWithCustomSessionParam(String databaseValue, String clientResultChunkSize)
      throws IOException {
    prepareTomlWithDatabase(
        databaseValue,
        null,
        null,
        Collections.singletonMap("CLIENT_RESULT_CHUNK_SIZE", clientResultChunkSize));
  }

  private void prepareConnectionConfigurationTomlFile() throws IOException {
    prepareConnectionConfigurationTomlFile(null, true, true);
  }

  private void prepareConnectionConfigurationTomlFile(Map<String, String> moreParameters)
      throws IOException {
    prepareConnectionConfigurationTomlFile(moreParameters, true, true);
  }

  private void prepareConnectionConfigurationTomlFile(
      Map<String, String> moreParameters,
      boolean onlyUserPermissionConnection,
      boolean onlyUserPermissionToken)
      throws IOException {
    prepareConnectionConfigurationTomlFile(
        moreParameters, onlyUserPermissionConnection, onlyUserPermissionToken, "token_from_file");
  }

  private void prepareConnectionConfigurationTomlFile(
      Map<String, String> moreParameters,
      boolean onlyUserPermissionConnection,
      boolean onlyUserPermissionToken,
      String token)
      throws IOException {
    Path path = Paths.get(tempPath.toString(), "connections.toml");
    Path filePath = createFilePathWithPermission(path, onlyUserPermissionConnection);
    File file = filePath.toFile();

    Map<String, Object> configuration = new HashMap<>();
    Map<String, Object> configurationParams = new HashMap<>();
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

  private void prepareTomlWithPortAndProtocol(String port, String protocol) throws IOException {
    Path path = Paths.get(tempPath.toString(), "connections.toml");
    Path filePath = createFilePathWithPermission(path, true);
    File file = filePath.toFile();

    Map<String, Object> configurationParams = new HashMap<>();
    configurationParams.put("account", "myorg-myaccount");
    configurationParams.put("user", "TOML_USER");
    configurationParams.put("password", "TOML_PASS");
    configurationParams.put("warehouse", "TOML_WH");
    if (port != null) {
      configurationParams.put("port", port);
    }
    if (protocol != null) {
      configurationParams.put("protocol", protocol);
    }

    Map<String, Object> configuration = new HashMap<>();
    configuration.put("default", configurationParams);
    tomlMapper.writeValue(file, configuration);
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
