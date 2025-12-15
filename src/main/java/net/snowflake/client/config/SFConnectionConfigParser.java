package net.snowflake.client.config;

import static net.snowflake.client.jdbc.SnowflakeUtil.convertSystemGetEnvToBooleanValue;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
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
  public static final String SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE =
      "SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE";

  private static final List<PosixFilePermission> REQUIRED_PERMISSIONS =
      Arrays.asList(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ);

  public static ConnectionParameters buildConnectionParameters(String connectionUrl)
      throws SnowflakeSQLException {
    String defaultConnectionName = getConnectionNameFromUrl(connectionUrl);
    if (SnowflakeUtil.isBlank(defaultConnectionName)) {
      defaultConnectionName =
          Optional.ofNullable(systemGetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY)).orElse(DEFAULT);
    }
    logger.debug("Attempting to load the configuration {} from toml file.", defaultConnectionName);
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

  static String getConnectionNameFromUrl(String connectionUrl) {

    Map<String, String> autoConfigJdbcUrlParameters =
        parseAutoConfigJdbcUrlParameters(connectionUrl);
    String connectionNameValue = autoConfigJdbcUrlParameters.get("connectionName");
    if (SnowflakeUtil.isBlank(connectionNameValue) || connectionNameValue.isEmpty()) {
      logger.debug("'connectionName' parameter is not configured");
      return "";
    } else {
      logger.debug("'connectionName' parameter is configured. The value is " + connectionNameValue);
      return connectionNameValue;
    }
  }

  private static Map<String, String> parseAutoConfigJdbcUrlParameters(String connectionUrl) {
    Map<String, String> paramMap = new HashMap<>();

    int queryStart = connectionUrl.indexOf('?');
    if (queryStart == -1) {
      return paramMap;
    }

    String query = connectionUrl.substring(queryStart + 1);
    String[] propertyPairs = query.split("&");

    for (String property : propertyPairs) {
      String[] peopertyKeyVal = property.split("=", 2);
      if (peopertyKeyVal.length == 2) {
        try {
          String key = URLDecoder.decode(peopertyKeyVal[0], "UTF-8");
          String value = URLDecoder.decode(peopertyKeyVal[1], "UTF-8");
          paramMap.put(key, value);
        } catch (UnsupportedEncodingException e) {
          logger.warn("Failed to decode a parameter {}. Ignored.", property);
        }
      }
    }

    return paramMap;
  }

  private static Map<String, String> loadDefaultConnectionConfiguration(
      String defaultConnectionName) throws SnowflakeSQLException {
    String configDirectory =
        Optional.ofNullable(systemGetEnv(SNOWFLAKE_HOME_KEY))
            .orElse(Paths.get(System.getProperty("user.home"), SNOWFLAKE_DIR).toString());
    Path configFilePath = Paths.get(configDirectory, "connections.toml");

    if (Files.exists(configFilePath)) {
      logger.debug(
          "Reading connection parameters from file {} using key: {}",
          configFilePath,
          defaultConnectionName);
      Map<String, Map> parametersMap = readParametersMap(configFilePath);
      Map<String, String> defaultConnectionParametersMap = parametersMap.get(defaultConnectionName);
      if (defaultConnectionParametersMap == null) {
        logger.debug("The Connection {} not found in connections.toml.", defaultConnectionName);
        throw new SnowflakeSQLException(
            "The Connection " + defaultConnectionName + " not found in connections.toml file.");
      } else {
        logger.debug("The Connection {} found in connections.toml.", defaultConnectionName);
      }
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

  static void verifyFilePermissionSecure(Path configFilePath)
      throws IOException, SnowflakeSQLException {
    final String fileName = "connections.toml";
    if (!isWindows()) {
      if (configFilePath.getFileName().toString().equals(fileName)) {
        boolean shouldSkipWarningForReadPermissions =
            convertSystemGetEnvToBooleanValue(
                SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE, false);
        PosixFileAttributeView posixFileAttributeView =
            Files.getFileAttributeView(configFilePath, PosixFileAttributeView.class);
        Set<PosixFilePermission> permissions =
            posixFileAttributeView.readAttributes().permissions();

        if (!shouldSkipWarningForReadPermissions) {
          boolean groupRead = permissions.contains(PosixFilePermission.GROUP_READ);
          boolean othersRead = permissions.contains(PosixFilePermission.OTHERS_READ);
          // Warning if readable by group/others (must be 600 or stricter)
          if (groupRead || othersRead) {
            logger.warn(
                "File %s is readable by group or others. Permissions should be 600 or stricter for maximum security.",
                configFilePath);
          }
        }

        boolean groupWrite = permissions.contains(PosixFilePermission.GROUP_WRITE);
        boolean othersWrite = permissions.contains(PosixFilePermission.OTHERS_WRITE);
        // Error if writable by group/others (must be 644 or stricter)
        if (groupWrite || othersWrite) {
          logger.error(
              "File %s is writable by group or others. Permissions must be 644 or stricter.",
              configFilePath);
          throw new SnowflakeSQLException(
              String.format(
                  "File %s is writable by group or others. Permissions must be 644 or stricter.",
                  configFilePath));
        }

        // Error if executable by anyone
        boolean ownerExec = permissions.contains(PosixFilePermission.OWNER_EXECUTE);
        boolean groupExec = permissions.contains(PosixFilePermission.GROUP_EXECUTE);
        boolean othersExec = permissions.contains(PosixFilePermission.OTHERS_EXECUTE);
        // Executable permission is not allowed
        if (ownerExec || groupExec || othersExec) {
          logger.error(
              "File %s is executable. Executable permission is not allowed.", configFilePath);
          throw new SnowflakeSQLException(
              String.format(
                  "File %s is executable. Executable permission is not allowed.", configFilePath));
        }
      } else {
        PosixFileAttributeView posixFileAttributeView =
            Files.getFileAttributeView(configFilePath, PosixFileAttributeView.class);
        if (!posixFileAttributeView.readAttributes().permissions().stream()
            .allMatch(o -> REQUIRED_PERMISSIONS.contains(o))) {
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
