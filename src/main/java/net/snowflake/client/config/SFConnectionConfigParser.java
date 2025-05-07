package net.snowflake.client.config;

import static net.snowflake.client.jdbc.SnowflakeUtil.convertSystemGetEnvToBooleanValue;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class SFConnectionConfigParser {

  private static final SFLogger logger = SFLoggerFactory.getLogger(SFConnectionConfigParser.class);
  private static final TomlMapper mapper = new TomlMapper();
  public static final String SNOWFLAKE_HOME_KEY = "SNOWFLAKE_HOME";
  public static final String SNOWFLAKE_DIR = ".snowflake";
  public static final String SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY =
      "SNOWFLAKE_DEFAULT_CONNECTION_NAME";
  public static final String DEFAULT = "default";
  public static final String SNOWFLAKE_TOKEN_FILE_PATH = "/snowflake/session/token";
  public static final String SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION =
      "SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION";

  public static ConnectionParameters buildConnectionParameters() throws SnowflakeSQLException {
    String defaultConnectionName =
        Optional.ofNullable(systemGetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY)).orElse(DEFAULT);
    Map<String, String> fileConnectionConfiguration =
        loadDefaultConnectionConfiguration(defaultConnectionName);

    if (fileConnectionConfiguration != null && !fileConnectionConfiguration.isEmpty()) {
      Properties connectionProperties = new Properties();
      connectionProperties.putAll(fileConnectionConfiguration);

      String url = createUrl(fileConnectionConfiguration);
      logger.debug("Url created using parameters from connection configuration file: {}", url);

      if ("oauth".equals(fileConnectionConfiguration.get("authenticator"))
          && fileConnectionConfiguration.get("token") == null) {
        Path path =
            Paths.get(
                Optional.ofNullable(fileConnectionConfiguration.get("token_file_path"))
                    .orElse(SNOWFLAKE_TOKEN_FILE_PATH));
        logger.debug("Token used in connect is read from file: {}", path);
        try {
          boolean shouldSkipTokenFilePermissionsVerification =
              convertSystemGetEnvToBooleanValue(SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION, false);
          if (!shouldSkipTokenFilePermissionsVerification) {
            verifyFilePermissionSecure(path);
          } else {
            logger.debug("Skip token file permissions verification");
          }
          String token = new String(Files.readAllBytes(path), Charset.defaultCharset());
          if (!token.isEmpty()) {
            putPropertyIfNotNull(connectionProperties, "token", token.trim());
          } else {
            throw new SnowflakeSQLException(
                "Non-empty token must be set when the authenticator type is OAUTH");
          }
        } catch (Exception ex) {
          throw new SnowflakeSQLException(ex, "There is a problem during reading token from file");
        }
      }
      return new ConnectionParameters(url, connectionProperties);
    } else {
      return null;
    }
  }

  private static Map<String, String> loadDefaultConnectionConfiguration(
      String defaultConnectionName) throws SnowflakeSQLException {
    String configDirectory =
        Optional.ofNullable(systemGetEnv(SNOWFLAKE_HOME_KEY))
            .orElse(Paths.get(System.getProperty("user.home"), SNOWFLAKE_DIR).toString());
    Path configFilePath = Paths.get(configDirectory, "connections.toml");

    if (Files.exists(configFilePath)) {
      logger.debug(
          "Reading connection parameters from file using key: {} []",
          configFilePath,
          defaultConnectionName);
      Map<String, Map> parametersMap = readParametersMap(configFilePath);
      Map<String, String> defaultConnectionParametersMap = parametersMap.get(defaultConnectionName);
      return defaultConnectionParametersMap;
    } else {
      logger.debug("Connection configuration file does not exist");
      return new HashMap<>();
    }
  }

  private static Map<String, Map> readParametersMap(Path configFilePath)
      throws SnowflakeSQLException {
    try {
      File file = new File(configFilePath.toUri());
      verifyFilePermissionSecure(configFilePath);
      return mapper.readValue(file, Map.class);
    } catch (IOException ex) {
      throw new SnowflakeSQLException(ex, "Problem during reading a configuration file.");
    }
  }

  private static void verifyFilePermissionSecure(Path configFilePath)
      throws IOException, SnowflakeSQLException {
    if (!isWindows()) {
      PosixFileAttributeView posixFileAttributeView =
          Files.getFileAttributeView(configFilePath, PosixFileAttributeView.class);
      if (!posixFileAttributeView.readAttributes().permissions().stream()
          .allMatch(
              o ->
                  Arrays.asList(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ)
                      .contains(o))) {
        logger.error(
            "Reading from file %s is not safe because file permissions are different than read/write for user",
            configFilePath);
        throw new SnowflakeSQLException(
            String.format(
                "Reading from file %s is not safe because file permissions are different than read/write for user",
                configFilePath));
      }
    }
  }

  private static String createUrl(Map<String, String> fileConnectionConfiguration)
      throws SnowflakeSQLException {
    Optional<String> maybeAccount = Optional.ofNullable(fileConnectionConfiguration.get("account"));
    Optional<String> maybeHost = Optional.ofNullable(fileConnectionConfiguration.get("host"));
    if (maybeAccount.isPresent()
        && maybeHost.isPresent()
        && !maybeHost.get().contains(maybeAccount.get())) {
      logger.warn(
          String.format(
              "Inconsistent host and account values in file configuration. ACCOUNT: {} , HOST: {}. The host value will be used.",
              maybeAccount.get(),
              maybeHost.get()));
    }
    String host =
        maybeHost.orElse(
            maybeAccount
                .map(acnt -> String.format("%s.snowflakecomputing.com", acnt))
                .orElse(null));
    if (host == null || host.isEmpty()) {
      logger.warn("Neither host nor account is specified in connection parameters");
      throw new SnowflakeSQLException(
          "Unable to connect because neither host nor account is specified in connection parameters");
    }
    logger.debug("Host created using parameters from connection configuration file: {}", host);
    String port = fileConnectionConfiguration.get("port");
    String protocol = fileConnectionConfiguration.get("protocol");
    if (isNullOrEmpty(port)) {
      if ("https".equals(protocol)) {
        port = "443";
      } else {
        port = "80";
      }
    }
    return String.format("jdbc:snowflake://%s:%s", host, port);
  }

  private static void putPropertyIfNotNull(Properties props, Object key, Object value) {
    if (key != null && value != null) {
      props.put(key, value);
    }
  }
}
