package net.snowflake.client.core.client_config;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import net.snowflake.client.jdbc.SnowflakeDriver;

public class SFClientConfigParser {
  public static final String SF_CLIENT_CONFIG_FILE_NAME = "sf_client_config.json";
  public static final String SF_CLIENT_CONFIG_ENV_NAME = "SF_CLIENT_CONFIG_FILE";

  /**
   * Construct SFClientConfig from client config file passed by user. This method searches the
   * config file in following order: 1. configFilePath param which is read from connection URL or
   * connection property. 2. Environment variable: SF_CLIENT_CONFIG_FILE containing full path to
   * sf_client_config file. 3. Searches for default config file name(sf_client_config.json under the
   * driver directory from where the driver gets loaded. 4. Searches for default config file
   * name(sf_client_config.json) under user home directory 5. Searches for default config file
   * name(sf_client_config.json) under tmp directory.
   *
   * @param configFilePath SF_CLIENT_CONFIG_FILE parameter read from connection URL or connection
   *     properties
   * @return SFClientConfig
   */
  public static SFClientConfig loadSFClientConfig(String configFilePath) {
    ObjectMapper objectMapper = new ObjectMapper();

    File configFile = null;
    try {
      if (configFilePath != null && !configFilePath.isEmpty()) {
        // 1. Try to read the file at  configFilePath.
        configFile = new File(configFilePath);
        return objectMapper.readValue(configFile, SFClientConfig.class);
      } else if (System.getenv().containsKey(SF_CLIENT_CONFIG_ENV_NAME)) {
        // 2. If SF_CLIENT_CONFIG_ENV_NAME is set, read from env.
        String envPath = systemGetEnv(SF_CLIENT_CONFIG_ENV_NAME);
        configFile = new File(envPath);
        return objectMapper.readValue(configFile, SFClientConfig.class);
      } else {
        // 3. Read SF_CLIENT_CONFIG_FILE_NAME from where jdbc jar is loaded.
        String driverLocation = getConfigFilePathFromJDBCJarLocation();
        if (Files.exists(Paths.get(driverLocation))) {
          configFile = new File(driverLocation);
          return objectMapper.readValue(configFile, SFClientConfig.class);
        }
        // 4. Read SF_CLIENT_CONFIG_FILE_NAME if it is present in user home directory.
        String userHomeFilePath =
            Paths.get(systemGetProperty("user.home"), SF_CLIENT_CONFIG_FILE_NAME).toString();
        if (Files.exists(Paths.get(userHomeFilePath))) {
          configFile = new File(userHomeFilePath);
          return objectMapper.readValue(configFile, SFClientConfig.class);
        }

        // 5. Read SF_CLIENT_CONFIG_FILE_NAME if it is present in tmpdir.
        String tmpFilePath =
            Paths.get(systemGetProperty("java.io.tmpdir"), SF_CLIENT_CONFIG_FILE_NAME).toString();
        if (Files.exists(Paths.get(tmpFilePath))) {
          configFile = new File(tmpFilePath);
          return objectMapper.readValue(configFile, SFClientConfig.class);
        }
      }
    } catch (IOException e) {
      System.err.println(
          String.format(
              "***** Error occured while reading sf client configuration file :%s, %s  *****",
              configFile.getPath(), e.getMessage()));
      e.printStackTrace(System.err);
    }
    // return null if none of the above conditions are satisfied.
    return null;
  }

  public static String getConfigFilePathFromJDBCJarLocation() {
    if (SnowflakeDriver.class.getProtectionDomain() != null
        && SnowflakeDriver.class.getProtectionDomain().getCodeSource() != null
        && SnowflakeDriver.class.getProtectionDomain().getCodeSource().getLocation() != null) {

      String jarPath =
          SnowflakeDriver.class.getProtectionDomain().getCodeSource().getLocation().getPath();
      // remove /snowflake-jdbc-3.13.29.jar from the path.
      return jarPath.substring(0, jarPath.lastIndexOf("/"))
          + File.separator
          + SF_CLIENT_CONFIG_FILE_NAME;
    }
    return "";
  }
}
