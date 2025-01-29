package net.snowflake.client.jdbc;

import static net.snowflake.client.core.SessionUtil.CLIENT_SFSQL;
import static net.snowflake.client.core.SessionUtil.JVM_PARAMS_TO_PARAMS;
import static net.snowflake.client.jdbc.SnowflakeUtil.isWindows;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import net.snowflake.client.config.SFClientConfig;
import net.snowflake.client.config.SFClientConfigParser;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.JDK14Logger;
import net.snowflake.client.log.SFLogLevel;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.log.SFToJavaLogMapper;
import net.snowflake.common.core.LoginInfoDTO;

/**
 * The default ConnectionHandler used by SnowflakeConnectionV(x). Unless a separate implementation
 * is provided, a DefaultConnectionHandler will be constructed automatically by the Connection
 * class.
 */
public class DefaultSFConnectionHandler implements SFConnectionHandler {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(DefaultSFConnectionHandler.class);

  private final SFSession sfSession;
  private final SnowflakeConnectString conStr;
  private final boolean skipOpen;

  /**
   * Constructs a DefaultConnectionHandler using a SnowflakeConnectString. This can be done by using
   * SnowflakeConnectString.parse(url, info), where url is a connection url and info is a
   * java.util.Properties
   *
   * @param conStr A SnowflakeConnectString object
   */
  public DefaultSFConnectionHandler(SnowflakeConnectString conStr) {
    this(conStr, false);
  }

  /**
   * Constructs a DefaultConnectionHandler using a SnowflakeConnectString. This can be done by using
   * SnowflakeConnectString.parse(url, info), where url is a connection url and info is a
   * java.util.Properties
   *
   * @param conStr A SnowflakeConnectString object
   * @param skipOpen Skip calling open() on the session (for test-use only)
   */
  public DefaultSFConnectionHandler(SnowflakeConnectString conStr, boolean skipOpen) {
    this.sfSession = new SFSession(this);
    this.conStr = conStr;
    this.skipOpen = skipOpen;
    sfSession.setSnowflakeConnectionString(conStr);
  }

  /**
   * Processes parameters given in the connection string. This extracts accountName, databaseName,
   * schemaName from the URL if it is specified there.
   *
   * @param conStr Connection string object
   * @return a map containing accountName, databaseName and schemaName if specified
   */
  public static Map<String, Object> mergeProperties(SnowflakeConnectString conStr) {
    conStr.getParameters().remove("SSL");
    conStr
        .getParameters()
        .put(
            "SERVERURL",
            conStr.getScheme() + "://" + conStr.getHost() + ":" + conStr.getPort() + "/");
    return conStr.getParameters();
  }

  @Override
  public boolean supportsAsyncQuery() {
    return true;
  }

  @Override
  public void initializeConnection(String url, Properties info) throws SQLException {
    initialize(conStr, LoginInfoDTO.SF_JDBC_APP_ID, SnowflakeDriver.implementVersion);
  }

  /** Returns the default SFSession client implementation. */
  @Override
  public SFBaseSession getSFSession() {
    return sfSession;
  }

  /** Returns the default SFStatement client implementation. */
  @Override
  public SFBaseStatement getSFStatement() {
    return new SFStatement(sfSession);
  }

  protected void initialize(SnowflakeConnectString conStr, String appID, String appVersion)
      throws SQLException {
    TelemetryService.getInstance().updateContext(conStr);

    try {
      // pass the parameters to sfSession
      initSessionProperties(conStr, appID, appVersion);
      setClientConfig();
      initLogger();
      logger.debug(
          "Trying to establish session, JDBC driver: {}", SnowflakeDriver.getJdbcJarname());
      if (!skipOpen) {
        sfSession.open();
      }

    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          sfSession, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
    }
  }

  private void setClientConfig() throws SnowflakeSQLLoggedException {
    Map<SFSessionProperty, Object> connectionPropertiesMap = sfSession.getConnectionPropertiesMap();
    String clientConfigFilePath =
        (String) connectionPropertiesMap.getOrDefault(SFSessionProperty.CLIENT_CONFIG_FILE, null);

    SFClientConfig sfClientConfig = sfSession.getSfClientConfig();
    if (sfClientConfig == null) {
      try {
        sfClientConfig = SFClientConfigParser.loadSFClientConfig(clientConfigFilePath);
      } catch (IOException e) {
        throw new SnowflakeSQLLoggedException(
            sfSession, ErrorCode.INTERNAL_ERROR, e.getMessage(), e.getCause());
      }
      sfSession.setSfClientConfig(sfClientConfig);
    }
  }

  /**
   * This method instantiates a JDK14Logger. This will be used if the java.util.logging.config.file
   * properties file is missing. The method performs the following actions: 1. Check if the
   * CLIENT_CONFIG_FILE is present. If it is, the method loads the logLevel and logPath from the
   * client config. 2. Check if the Tracing parameter is present in the URL or connection
   * properties. If it is, the method will overwrite the logLevel obtained from step 1. 3.
   * Instantiate java.util.logging with the specified logLevel and logPath. 4. If both the logLevel
   * and logPath are null, this method doesn't do anything.
   */
  private void initLogger() throws SnowflakeSQLLoggedException {
    if (logger instanceof JDK14Logger
        && systemGetProperty("java.util.logging.config.file") == null) {
      Map<SFSessionProperty, Object> connectionPropertiesMap =
          sfSession.getConnectionPropertiesMap();
      String tracingLevelFromConnectionProp =
          (String) connectionPropertiesMap.getOrDefault(SFSessionProperty.TRACING, null);

      Level logLevel = null;
      String logPattern = "%h/snowflake_jdbc%u.log"; // default pattern.
      SFClientConfig sfClientConfig = sfSession.getSfClientConfig();

      if (sfClientConfig != null) {
        String logPathFromConfig = sfClientConfig.getCommonProps().getLogPath();
        logPattern = constructLogPattern(logPathFromConfig);
        String levelStr = sfClientConfig.getCommonProps().getLogLevel();
        SFLogLevel sfLogLevel = SFLogLevel.getLogLevel(levelStr);
        logLevel = SFToJavaLogMapper.toJavaUtilLoggingLevel(sfLogLevel);
      }

      if (tracingLevelFromConnectionProp != null) {
        // Log level from connection param will overwrite the log level from sf config file.
        logLevel = Level.parse(tracingLevelFromConnectionProp.toUpperCase());
      }

      if (logLevel != null && logPattern != null) {
        try {
          logger.debug("Setting logger with log level {} and log pattern {}", logLevel, logPattern);
          JDK14Logger.instantiateLogger(logLevel, logPattern);
        } catch (IOException ex) {
          throw new SnowflakeSQLLoggedException(
              sfSession, ErrorCode.INTERNAL_ERROR, ex.getMessage());
        }
        if (sfClientConfig != null) {
          logger.debug(
              "SF Client config found at location: {}.", sfClientConfig.getConfigFilePath());
        }
        logger.debug(
            "Instantiating JDK14Logger with level: {}, output path: {}", logLevel, logPattern);
      }
    }
  }

  private String constructLogPattern(String logPathFromConfig) throws SnowflakeSQLLoggedException {
    if (JDK14Logger.STDOUT.equalsIgnoreCase(logPathFromConfig)) {
      return JDK14Logger.STDOUT;
    }

    String logPattern = "%t/snowflake_jdbc%u.log"; // java.tmpdir

    Path logPath;
    if (logPathFromConfig != null && !logPathFromConfig.isEmpty()) {
      // Get log path from configuration
      logPath = Paths.get(logPathFromConfig);
      if (!Files.exists(logPath)) {
        try {
          Files.createDirectories(logPath);
        } catch (IOException ex) {
          throw new SnowflakeSQLLoggedException(
              sfSession,
              ErrorCode.INTERNAL_ERROR,
              String.format(
                  "Unable to create log path mentioned in configfile %s ,%s",
                  logPathFromConfig, ex.getMessage()));
        }
      }
    } else {
      // Get log path from home directory
      String homePath = systemGetProperty("user.home");
      if (homePath == null || homePath.isEmpty()) {
        throw new SnowflakeSQLLoggedException(
            sfSession,
            ErrorCode.INTERNAL_ERROR,
            String.format(
                "Log path not set in configfile %s and home directory not set.",
                logPathFromConfig));
      }
      logPath = Paths.get(homePath);
    }

    Path path = createLogPathSubDirectory(logPath);

    logPattern = Paths.get(path.toString(), "snowflake_jdbc%u.log").toString();
    return logPattern;
  }

  private Path createLogPathSubDirectory(Path logPath) throws SnowflakeSQLLoggedException {
    Path path = Paths.get(logPath.toString(), "jdbc");
    if (!Files.exists(path)) {
      createLogFolder(path);
    } else {
      checkLogFolderPermissions(path);
    }
    return path;
  }

  private void createLogFolder(Path path) throws SnowflakeSQLLoggedException {
    try {
      if (Constants.getOS() == Constants.OS.WINDOWS) {
        Files.createDirectories(path);
      } else {
        Files.createDirectories(
            path,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
      }
    } catch (IOException ex) {
      throw new SnowflakeSQLLoggedException(
          sfSession,
          ErrorCode.INTERNAL_ERROR,
          String.format(
              "Unable to create jdbc subfolder in configfile %s ,%s",
              path.toString(), ex.getMessage(), ex.getCause()));
    }
  }

  private void checkLogFolderPermissions(Path path) throws SnowflakeSQLLoggedException {
    if (!isWindows()) {
      try {
        Set<PosixFilePermission> folderPermissions = Files.getPosixFilePermissions(path);
        if (folderPermissions.contains(PosixFilePermission.GROUP_WRITE)
            || folderPermissions.contains(PosixFilePermission.GROUP_READ)
            || folderPermissions.contains(PosixFilePermission.GROUP_EXECUTE)
            || folderPermissions.contains(PosixFilePermission.OTHERS_WRITE)
            || folderPermissions.contains(PosixFilePermission.OTHERS_READ)
            || folderPermissions.contains(PosixFilePermission.OTHERS_EXECUTE)) {
          logger.warn(
              "Access permission for the logs directory '{}' is currently {} and is potentially "
                  + "accessible to users other than the owner of the logs directory.",
              path.toString(),
              folderPermissions.toString());
        }
      } catch (IOException ex) {
        throw new SnowflakeSQLLoggedException(
            sfSession,
            ErrorCode.INTERNAL_ERROR,
            String.format(
                "Unable to get permissions of log directory %s ,%s",
                path.toString(), ex.getMessage(), ex.getCause()));
      }
    }
  }

  private void initSessionProperties(SnowflakeConnectString conStr, String appID, String appVersion)
      throws SFException {
    Map<String, Object> properties = mergeProperties(conStr);

    for (Map.Entry<String, Object> property : properties.entrySet()) {
      if ("CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY".equals(property.getKey())) {
        try {
          Object v0 = property.getValue();
          int intV;
          if (v0 instanceof Integer) {
            intV = (Integer) v0;
          } else {
            intV = Integer.parseInt((String) v0);
          }
          if (intV > 3600) {
            properties.replace(property.getKey(), "3600");
          }
          if (intV < 900) {
            properties.replace(property.getKey(), "900");
          }
        } catch (NumberFormatException ex) {
          logger.warn(
              "Invalid data type for CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY: {}",
              property.getValue());
          continue;
        }
      } else if (CLIENT_SFSQL.equals(property.getKey())) {
        Object v0 = property.getValue();
        boolean booleanV = v0 instanceof Boolean ? (Boolean) v0 : Boolean.parseBoolean((String) v0);
        sfSession.setSfSQLMode(booleanV);
      }
      sfSession.addSFSessionProperty(property.getKey(), property.getValue());
    }
    sfSession.overrideConsoleHandlerWhenNecessary();

    // populate app id and version
    sfSession.addProperty(SFSessionProperty.APP_ID, appID);
    sfSession.addProperty(SFSessionProperty.APP_VERSION, appVersion);

    // Set the corresponding session parameters to the JVM properties
    for (Map.Entry<String, String> entry : JVM_PARAMS_TO_PARAMS.entrySet()) {
      String value = systemGetProperty(entry.getKey());
      if (value != null && !sfSession.containProperty(entry.getValue())) {
        sfSession.addSFSessionProperty(entry.getValue(), value);
      }
    }
  }

  @Override
  public ResultSet createResultSet(String queryID, Statement statement) throws SQLException {
    SFAsyncResultSet rs = new SFAsyncResultSet(queryID, statement);
    rs.setSession(sfSession);
    rs.setStatement(statement);
    return rs;
  }

  @Override
  public SnowflakeBaseResultSet createResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException {
    return new SnowflakeResultSetV1(resultSet, statement);
  }

  @Override
  public SnowflakeBaseResultSet createAsyncResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException {
    return new SFAsyncResultSet(resultSet, statement);
  }

  @Override
  public SFBaseFileTransferAgent getFileTransferAgent(String command, SFBaseStatement statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException {
    if (!(statement instanceof SFStatement)) {
      throw new SnowflakeSQLException(
          "getFileTransferAgent() called with an incompatible SFBaseStatement type. Requires an"
              + " SFStatement.");
    }
    return new SnowflakeFileTransferAgent(command, sfSession, (SFStatement) statement);
  }
}
