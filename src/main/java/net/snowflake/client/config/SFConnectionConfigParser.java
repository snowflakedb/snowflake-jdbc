package net.snowflake.client.config;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import com.google.common.base.Strings;
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
  private static TomlMapper mapper = new TomlMapper();

  private static Map<String, String> loadDefaultConnectionConfiguration(
      String defaultConnectionName) throws SnowflakeSQLException {
    String configDirectory =
        Optional.ofNullable(systemGetEnv("SNOWFLAKE_HOME"))
            .orElse(Paths.get(System.getProperty("user.home"), ".snowflake").toString());
    Path configFilePath = Paths.get(configDirectory, "connections.toml");

    if (Files.exists(configFilePath)) {
      logger.debug("Reading connection parameters from file: {}", configFilePath);
      Map<String, Map> data = readParametersMap(configFilePath);
      Map<String, String> defaultConnectionParametersMap = data.get(defaultConnectionName);
      if (defaultConnectionParametersMap == null || defaultConnectionParametersMap.isEmpty()) {
        throw new SnowflakeSQLException(
            String.format(
                "Connection configuration with name %s does not contains paramaters",
                defaultConnectionName));
      }

      return defaultConnectionParametersMap;
    } else {
      return new HashMap<>();
    }
  }

  private static Map<String, Map> readParametersMap(Path configFilePath)
      throws SnowflakeSQLException {
    try {
      File file = new File(configFilePath.toUri());
      varifyFilePermissionSecure(configFilePath);
      return mapper.readValue(file, Map.class);
    } catch (IOException e) {
      throw new SnowflakeSQLException(
          "Problem during reading a configuration file: " + e.getMessage());
    }
  }

  private static void varifyFilePermissionSecure(Path configFilePath)
      throws IOException, SnowflakeSQLException {
    PosixFileAttributeView posixFileAttributeView =
        Files.getFileAttributeView(configFilePath, PosixFileAttributeView.class);
    if (!posixFileAttributeView.readAttributes().permissions().stream()
        .allMatch(
            o ->
                Arrays.asList(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ)
                    .contains(o))) {
      logger.error("Reading from file {} is not secure", configFilePath);
      throw new SnowflakeSQLException(
          String.format("Reading from file %s is not secure", configFilePath));
    }
  }

  public static ConnectionParameters buildConnectionParameters() throws SnowflakeSQLException {
    String defaultConnectionName =
        Optional.ofNullable(systemGetEnv("SNOWFLAKE_DEFAULT_CONNECTION_NAME")).orElse("default");
    Map<String, String> fileConnectionConfiguration =
        loadDefaultConnectionConfiguration(defaultConnectionName);

    Properties conectionProperties = new Properties();
    conectionProperties.putAll(fileConnectionConfiguration);

    String url =
        Optional.ofNullable(fileConnectionConfiguration.get("account"))
            .map(ac -> createUrl(ac, fileConnectionConfiguration))
            .orElse(null);

    if ("oauth".equals(fileConnectionConfiguration.get("authenticator"))
        && fileConnectionConfiguration.get("token") == null) {
      Path path =
          Paths.get(
              Optional.ofNullable(fileConnectionConfiguration.get("token_file_path"))
                  .orElse("/snowflake/session/token"));
      logger.debug("Token used in connect is read from file: {}", path);
      try {
        String token = new String(Files.readAllBytes(path), Charset.defaultCharset());
        if (!token.isEmpty()) {
          putPropertyIfNotNull(conectionProperties, "token", token.trim());
        } else {
          logger.warn("The token has empty value");
        }
      } catch (Exception e) {
        throw new SnowflakeSQLException(
            "There is a problem during reading token from file", e.getMessage());
      }
    }

    return new ConnectionParameters(url, conectionProperties);
  }

  private static String createUrl(String ac, Map<String, String> fileConnectionConfiguration) {
    String host = String.format("%s.snowflakecomputing.com", ac);
    String port = fileConnectionConfiguration.get("port");
    String protocol = fileConnectionConfiguration.get("protocol");
    if (Strings.isNullOrEmpty(port)) {
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
