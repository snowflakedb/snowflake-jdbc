package net.snowflake.client.jdbc;

import static net.snowflake.client.core.SessionUtil.CLIENT_SFSQL;
import static net.snowflake.client.core.SessionUtil.JVM_PARAMS_TO_PARAMS;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
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
    this.sfSession = new SFSession();
    this.conStr = conStr;
    this.skipOpen = skipOpen;
    sfSession.setSnowflakeConnectionString(conStr);
  }

  /**
   * Processes parameters given in the connection string. This extracts accountName, databaseName,
   * schemaName from the URL if it is specified there.
   *
   * @param conStr Connection string object
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
    initialize(conStr);
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

  private void initialize(SnowflakeConnectString conStr) throws SQLException {
    logger.debug(
        "Trying to establish session, JDBC driver version: {}", SnowflakeDriver.implementVersion);
    TelemetryService.getInstance().updateContext(conStr);

    try {
      // pass the parameters to sfSession
      initSessionProperties(conStr);
      if (!skipOpen) {
        sfSession.open();
      }

    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          sfSession, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
    }
  }

  private void initSessionProperties(SnowflakeConnectString conStr) throws SFException {
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
          logger.info(
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

    // populate app id and version
    sfSession.addProperty(SFSessionProperty.APP_ID, LoginInfoDTO.SF_JDBC_APP_ID);
    sfSession.addProperty(SFSessionProperty.APP_VERSION, SnowflakeDriver.implementVersion);

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
    SFAsyncResultSet rs = new SFAsyncResultSet(queryID);
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
          "getFileTransferAgent() called with an incompatible SFBaseStatement type. Requires an SFStatement.");
    }
    return new SnowflakeFileTransferAgent(command, sfSession, (SFStatement) statement);
  }
}
