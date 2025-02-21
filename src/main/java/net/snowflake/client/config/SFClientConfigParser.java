package net.snowflake.client.config;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.core.Constants;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class SFClientConfigParser {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFClientConfigParser.class);
  public static final String SF_CLIENT_CONFIG_FILE_NAME = "sf_client_config.json";
  public static final String SF_CLIENT_CONFIG_ENV_NAME = "SF_CLIENT_CONFIG_FILE";

  /**
   * Construct SFClientConfig from client config file passed by user. This method searches the
   * config file in following order: 1. configFilePath param which is read from connection URL or
   * connection property. 2. Environment variable: SF_CLIENT_CONFIG_FILE containing full path to
   * sf_client_config file. 3. Searches for default config file name(sf_client_config.json under the
   * driver directory from where the driver gets loaded. 4. Searches for default config file
   * name(sf_client_config.json) under user home directory.
   *
   * @param configFilePath SF_CLIENT_CONFIG_FILE parameter read from connection URL or connection
   *     properties
   * @return SFClientConfig
   * @throws IOException if exception encountered when reading config file.
   */
  public static SFClientConfig loadSFClientConfig(String configFilePath) throws IOException {
    if (configFilePath != null) {
      logger.info("Attempting to enable easy logging with file path {}", configFilePath);
    }
    String derivedConfigFilePath = null;
    if (configFilePath != null && !configFilePath.isEmpty()) {
      // 1. Try to read the file at  configFilePath.
      logger.info("Using config file specified from connection string: {}", configFilePath);
      derivedConfigFilePath = configFilePath;
    } else if (System.getenv().containsKey(SF_CLIENT_CONFIG_ENV_NAME)) {
      // 2. If SF_CLIENT_CONFIG_ENV_NAME is set, read from env.
      String filePath = systemGetEnv(SF_CLIENT_CONFIG_ENV_NAME);
      logger.info("Using config file specified from environment variable: {}", filePath);
      derivedConfigFilePath = filePath;
    } else {
      // 3. Read SF_CLIENT_CONFIG_FILE_NAME from where jdbc jar is loaded.
      String driverLocation =
          Paths.get(getConfigFilePathFromJDBCJarLocation(), SF_CLIENT_CONFIG_FILE_NAME).toString();
      if (Files.exists(Paths.get(driverLocation))) {
        logger.info("Using config file specified from driver directory: {}", driverLocation);
        derivedConfigFilePath = driverLocation;
      } else {
        // 4. Read SF_CLIENT_CONFIG_FILE_NAME if it is present in user home directory.
        String homeDirectory = systemGetProperty("user.home");
        if (homeDirectory != null) {
          String userHomeFilePath = Paths.get(homeDirectory, SF_CLIENT_CONFIG_FILE_NAME).toString();
          if (Files.exists(Paths.get(userHomeFilePath))) {
            logger.info("Using config file specified from home directory: {}", userHomeFilePath);
            derivedConfigFilePath = userHomeFilePath;
          }
        }
      }
    }
    if (derivedConfigFilePath != null) {
      try {
        checkConfigFilePermissions(derivedConfigFilePath);

        File configFile = new File(derivedConfigFilePath);
        ObjectMapper objectMapper = new ObjectMapper();
        SFClientConfig clientConfig = objectMapper.readValue(configFile, SFClientConfig.class);
        logger.debug(
            "Reading values logLevel {} and logPath {} from client configuration",
            clientConfig.getCommonProps().getLogLevel(),
            clientConfig.getCommonProps().getLogPath());

        Set<String> unknownParams = clientConfig.getUnknownParamKeys();
        if (!unknownParams.isEmpty()) {
          for (String unknownParam : unknownParams) {
            logger.warn("Unknown field from config: {}", unknownParam);
          }
        }
        clientConfig.setConfigFilePath(derivedConfigFilePath);

        return clientConfig;
      } catch (IOException e) {
        String customErrorMessage =
            "Error while reading config file at location: " + derivedConfigFilePath;
        throw new IOException(customErrorMessage, e);
      }
    }
    // return null if none of the above conditions are satisfied.
    return null;
  }

  public static String getConfigFilePathFromJDBCJarLocation() {
    try {
      if (SnowflakeDriver.class.getProtectionDomain() != null
          && SnowflakeDriver.class.getProtectionDomain().getCodeSource() != null
          && SnowflakeDriver.class.getProtectionDomain().getCodeSource().getLocation() != null) {

        String jarPath =
            SnowflakeDriver.class.getProtectionDomain().getCodeSource().getLocation().getPath();

        // remove /snowflake-jdbc-3.13.29.jar and anything that follows it from the path.
        String updatedPath = new File(jarPath).getParentFile().getPath();

        if (systemGetProperty("os.name") != null
            && systemGetProperty("os.name").toLowerCase().startsWith("windows")) {
          updatedPath = convertToWindowsPath(updatedPath);
        }
        return updatedPath;
      }

      return "";
    } catch (Exception ex) {
      // return empty path and move to step 4 of loadSFClientConfig()
      return "";
    }
  }

  private static void checkConfigFilePermissions(String derivedConfigFilePath) throws IOException {
    try {
      if (Constants.getOS() != Constants.OS.WINDOWS) {
        // Check permissions of config file
        if (checkGroupOthersWritePermissions(derivedConfigFilePath)) {
          String error =
              String.format(
                  "Error due to other users having permission to modify the config file: %s",
                  derivedConfigFilePath);
          // TODO: SNOW-1503722 to change warning log to throw an error instead
          logger.warn(error);
        }
      }
    } catch (IOException e) {
      throw e;
    }
  }

  static Boolean checkGroupOthersWritePermissions(String configFilePath) throws IOException {
    Set<PosixFilePermission> folderPermissions =
        Files.getPosixFilePermissions(Paths.get(configFilePath));
    return folderPermissions.contains(PosixFilePermission.GROUP_WRITE)
        || folderPermissions.contains(PosixFilePermission.OTHERS_WRITE);
  }

  static String convertToWindowsPath(String filePath) {
    // Find the Windows file path pattern: ex) C:\ or D:\
    Pattern windowsFilePattern = Pattern.compile("[C-Z]:[\\\\/]");
    Matcher matcher = windowsFilePattern.matcher(filePath);
    String prefix = "";

    // Path translation for windows
    if (filePath.startsWith("/")) {
      filePath = filePath.substring(1);
    } else if (filePath.startsWith("file:\\")) {
      filePath = filePath.substring(6);
    } else if (filePath.startsWith("nested:\\")) {
      filePath = filePath.substring(8);
    } else if (filePath.startsWith("\\")) {
      filePath = filePath.substring(2);
    } else if (matcher.find() && matcher.start() != 0) {
      prefix = filePath.substring(0, matcher.start());
      filePath = filePath.substring(matcher.start());
    }
    filePath = prefix + filePath.replace("/", "\\");
    return filePath;
  }
}
