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

  private static Map<String, String> loadDefaultConnectionConfiguration(
      String defaultConnectionName) throws SnowflakeSQLException {
    String configDirectory =
        Optional.ofNullable(systemGetEnv("SNOWFLAKE_HOME"))
            .orElse(Paths.get(System.getProperty("user.home"), ".snowflake").toString());
    Path configFilePath = Paths.get(configDirectory, "connections.toml");
    logger.debug("Reading connection parameters from file: " + configFilePath);
    TomlMapper mapper = new TomlMapper();
    Map<String, Map> data;
    try {
      data = mapper.readValue(new File(configFilePath.toUri()), Map.class);
    } catch (IOException e) {
      throw new SnowflakeSQLException(
          "Problem during reading a configuration file: " + e.getMessage());
    }

    Map<String, String> defaultConnectionParametersMap = data.get(defaultConnectionName);
    if (defaultConnectionParametersMap == null || defaultConnectionParametersMap.isEmpty()) {
      throw new SnowflakeSQLException(
          String.format(
              "Connection configuration with name %s does not exist", defaultConnectionName));
    }

    return defaultConnectionParametersMap;
  }

  public static ConnectionParameters buildConnectionParameters(String defaultConnectionName)
      throws SnowflakeSQLException {
    Map<String, String> fileConnectionConfiguration =
        loadDefaultConnectionConfiguration(defaultConnectionName);

    Properties conectionProperties = new Properties();
    conectionProperties.putAll(fileConnectionConfiguration);

    String host =
        Optional.ofNullable(fileConnectionConfiguration.get("account"))
            .map(ac -> ac + ".snowflakecomputing.com")
            .orElseThrow(
                () ->
                    new SnowflakeSQLException(
                        "Connection configuration is invalid. Account must be set."));

    String port = fileConnectionConfiguration.get("port");
    ;
    String protocol = fileConnectionConfiguration.get("protocol");
    if (Strings.isNullOrEmpty(port)) {
      if ("https".equals(protocol)) {
        port = "443";
      } else {
        port = "80";
      }
    }
    String uri = String.format("jdbc:snowflake://%s:%s", host, port);

    if (fileConnectionConfiguration.containsKey("token_file_path")) {
      Path path =
          Paths.get(
              Optional.ofNullable(fileConnectionConfiguration.get("token_file_path"))
                  .orElse("/snowflake/session/token"));
      try {
        String token = new String(Files.readAllBytes(path), Charset.defaultCharset());
        if (!token.isEmpty()) {
          putPropertyIfNotNull(conectionProperties, "token", token.trim());
        }
      } catch (Exception e) {
        throw new SnowflakeSQLException(
            "There is a problem during reading token from file", e.getMessage());
      }
    }

    return new ConnectionParameters(uri, conectionProperties);
  }

  private static void putPropertyIfNotNull(Properties props, Object key, Object value) {
    if (key != null && value != null) {
      props.put(key, value);
    }
  }
}
