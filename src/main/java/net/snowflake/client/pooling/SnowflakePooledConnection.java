package net.snowflake.client.pooling;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Snowflake implementation of pooled connection */
public class SnowflakePooledConnection implements PooledConnection {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakePooledConnection.class);

  /** physical connection, an instance of SnowflakeConnectionV1 class */
  private Connection physicalConnection;

  /** list of event listener registered to listen for connection event */
  private final Set<ConnectionEventListener> eventListeners;

  SnowflakePooledConnection(Connection physicalConnection) throws SQLException {
    this.physicalConnection = physicalConnection;

    SnowflakeConnectionV1 sfConnection = physicalConnection.unwrap(SnowflakeConnectionV1.class);
    logger.debug("Creating new pooled connection with session id: {}", sfConnection.getSessionID());

    this.eventListeners = new HashSet<>();
  }

  @Override
  public Connection getConnection() throws SQLException {
    SnowflakeConnectionV1 sfConnection = physicalConnection.unwrap(SnowflakeConnectionV1.class);
    logger.debug(
        "Creating new Logical Connection based on pooled connection with session id: {}",
        sfConnection.getSessionID());
    return new LogicalConnection(this);
  }

  Connection getPhysicalConnection() {
    return physicalConnection;
  }

  /** Fire a connection has been closed event to event listener */
  void fireConnectionCloseEvent() {
    for (ConnectionEventListener connectionEventListener : eventListeners) {
      connectionEventListener.connectionClosed(new ConnectionEvent(this));
    }
  }

  void fireConnectionErrorEvent(SQLException e) {
    for (ConnectionEventListener connectionEventListener : eventListeners) {
      connectionEventListener.connectionErrorOccurred(new ConnectionEvent(this, e));
    }
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener eventListener) {
    this.eventListeners.add(eventListener);
  }

  @Override
  public void close() throws SQLException {
    if (this.physicalConnection != null) {
      SnowflakeConnectionV1 sfConnection = physicalConnection.unwrap(SnowflakeConnectionV1.class);
      logger.debug("Closing pooled connection with session id: {}", sfConnection.getSessionID());
      this.physicalConnection.close();
      this.physicalConnection = null;
    }

    eventListeners.clear();
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener eventListener) {
    this.eventListeners.remove(eventListener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener eventListener) {
    // do nothing for now
  }

  @Override
  public void removeStatementEventListener(StatementEventListener eventListener) {
    // do nothing for now
  }
}
