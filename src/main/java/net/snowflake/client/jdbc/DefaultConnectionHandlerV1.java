package net.snowflake.client.jdbc;

import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.SFLogger;
import net.snowflake.common.core.LoginInfoDTO;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Map;
import java.util.Properties;

import static net.snowflake.client.core.SessionUtil.CLIENT_SFSQL;
import static net.snowflake.client.core.SessionUtil.JVM_PARAMS_TO_PARAMS;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

/**
 * The default ConnectionHandler used by SnowflakeConnectionV(x). Unless a separate implementation
 * is provided, a DefaultConnectionHandler will be constructed automatically by the Connection
 * class.
 */
public class DefaultConnectionHandlerV1 implements ConnectionHandlerV1 {

  private final SFSession sfSession;
  private final SFLogger logger;
  private SnowflakeConnectString conStr;
  private boolean skipOpen;

  /**
   * Constructs a DefaultConnectionHandler using a SnowflakeConnectString. This can be done by using
   * SnowflakeConnectString.parse(url, info), where url is a connection url and info is a
   * java.util.Properties
   *
   * @param conStr A SnowflakeConnectString object
   */
  public DefaultConnectionHandlerV1(SnowflakeConnectString conStr, SFLogger logger) {
    this(conStr, logger, false);
  }

  /**
   * Constructs a DefaultConnectionHandler using a SnowflakeConnectString. This can be done by using
   * SnowflakeConnectString.parse(url, info), where url is a connection url and info is a
   * java.util.Properties
   *
   * @param conStr A SnowflakeConnectString object
   */
  public DefaultConnectionHandlerV1(
      SnowflakeConnectString conStr, SFLogger logger, boolean skipOpen) {
    this.sfSession = new SFSession();
    this.conStr = conStr;
    this.logger = logger;
    this.skipOpen = skipOpen;
    sfSession.setSnowflakeConnectionString(conStr);
  }


  @Override
  public void initializeConnection(String url, Properties info) throws SQLException {
    initialize(conStr);
  }

  /** Returns the default SFSession client implementation. */
  @Override
  public SessionHandler getSessionHandler() {
    return sfSession;
  }

  /** Returns the default SFStatement client implementation. */
  @Override
  public StatementHandler getStatementHandler() {
    return new SFStatement(sfSession);
  }

  /**
   * Returns the default SnowflakeFileTransferAgent implementation for the client.
   *
   * @param command The command to parse for this file transfer (e.g., PUT/GET)
   * @param statement The statement to use for this file transfer
   */
  @Override
  public FileTransferHandler getFileTransferHandler(String command, StatementHandler statement)
      throws SQLNonTransientConnectionException, SnowflakeSQLException {
    if (!(statement instanceof SFStatement)) {
      throw new SQLNonTransientConnectionException("Internal error: Invalid SFStatement type.");
    }
    return new SnowflakeFileTransferAgent(command, sfSession, (SFStatement) statement);
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
      sfSession.addProperty(property.getKey(), property.getValue());
    }

    // populate app id and version
    sfSession.sessionProperties().addProperty(SFConnectionProperty.APP_ID, LoginInfoDTO.SF_JDBC_APP_ID);
    sfSession.sessionProperties().addProperty(SFConnectionProperty.APP_VERSION, SnowflakeDriver.implementVersion);

    // Set the corresponding session parameters to the JVM properties
    for (Map.Entry<String, String> entry : JVM_PARAMS_TO_PARAMS.entrySet()) {
      String value = systemGetProperty(entry.getKey());
      if (value != null && !sfSession.containProperty(entry.getValue())) {
        sfSession.addProperty(entry.getValue(), value);
      }
    }
  }


  /**
   * Processes parameters given in the connection string. This extracts accountName, databaseName,
   * schemaName from the URL if it is specified there, where the URL is of the form:
   *
   * <p>jdbc:snowflake://host:port/?user=v&password=v&account=v&
   * db=v&schema=v&ssl=v&[passcode=v|passcodeInPassword=on]
   *
   * @param conStr Connection string object
   */
  static Map<String, Object> mergeProperties(SnowflakeConnectString conStr) {
    conStr.getParameters().remove("SSL");
    conStr
            .getParameters()
            .put(
                    "SERVERURL",
                    conStr.getScheme() + "://" + conStr.getHost() + ":" + conStr.getPort() + "/");
    return conStr.getParameters();
  }

  /** For test-use only. */

  /**
   * Get an instance of a ResultSet object
   *
   * @param queryID
   * @return
   * @throws SQLException
   */
  @Override
  public ResultSet createResultSet(String queryID, Connection connection) throws SQLException {
    SFAsyncResultSet rs = new SFAsyncResultSet(queryID);
    rs.setSession(sfSession);
    rs.setStatement(connection.createStatement());
    return rs;
  }
}
