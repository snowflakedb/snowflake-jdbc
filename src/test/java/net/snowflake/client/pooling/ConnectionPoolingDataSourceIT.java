package net.snowflake.client.pooling;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
public class ConnectionPoolingDataSourceIT extends AbstractDriverIT {
  @Test
  public void testPooledConnection() throws SQLException {
    Map<String, String> properties = getConnectionParameters();

    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();

    poolDataSource.setUrl(properties.get("uri"));
    poolDataSource.setPortNumber(Integer.parseInt(properties.get("port")));
    poolDataSource.setSsl("on".equals(properties.get("ssl")));
    poolDataSource.setAccount(properties.get("account"));
    poolDataSource.setUser(properties.get("user"));
    poolDataSource.setPassword(properties.get("password"));

    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    TestingConnectionListener listener = new TestingConnectionListener();
    pooledConnection.addConnectionEventListener(listener);

    try (Connection connection = pooledConnection.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("select 1");

      SQLException e =
          assertThrows(SQLException.class, () -> connection.setCatalog("nonexistent_database"));
      assertThat(e.getErrorCode(), is(2043));

      // should not close underlying physical connection
      // and fire connection closed events
    }

    List<ConnectionEvent> connectionClosedEvents = listener.getConnectionClosedEvents();
    List<ConnectionEvent> connectionErrorEvents = listener.getConnectionErrorEvents();

    // assert connection close event
    assertThat(connectionClosedEvents.size(), is(1));
    ConnectionEvent closedEvent = connectionClosedEvents.get(0);
    assertThat(closedEvent.getSQLException(), is(nullValue()));
    assertThat(closedEvent.getSource(), instanceOf(SnowflakePooledConnection.class));
    assertThat((PooledConnection) closedEvent.getSource(), sameInstance(pooledConnection));

    // assert connection error event
    assertThat(connectionErrorEvents.size(), is(1));
    ConnectionEvent errorEvent = connectionErrorEvents.get(0);

    assertThat(errorEvent.getSource(), instanceOf(SnowflakePooledConnection.class));
    assertThat((PooledConnection) errorEvent.getSource(), sameInstance(pooledConnection));
    // 2043 is the error code for object not existed
    assertThat(errorEvent.getSQLException().getErrorCode(), is(2043));

    // assert physical connection is not closed
    Connection physicalConnection =
        ((SnowflakePooledConnection) pooledConnection).getPhysicalConnection();

    assertThat(physicalConnection.isClosed(), is(false));

    pooledConnection.removeConnectionEventListener(listener);

    // will close physical connection
    pooledConnection.close();
    assertThat(physicalConnection.isClosed(), is(true));
  }

  @Test
  public void testPooledConnectionUsernamePassword() throws SQLException {
    Map<String, String> properties = getConnectionParameters();

    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();

    poolDataSource.setUrl(properties.get("uri"));
    poolDataSource.setPortNumber(Integer.parseInt(properties.get("port")));
    poolDataSource.setSsl("on".equals(properties.get("ssl")));
    poolDataSource.setAccount(properties.get("account"));

    PooledConnection pooledConnection =
        poolDataSource.getPooledConnection(properties.get("user"), properties.get("password"));
    TestingConnectionListener listener = new TestingConnectionListener();
    pooledConnection.addConnectionEventListener(listener);

    try (Connection connection = pooledConnection.getConnection()) {
      connection.createStatement().execute("select 1");
    }
    pooledConnection.close();
  }

  private static class TestingConnectionListener implements ConnectionEventListener {
    private List<ConnectionEvent> connectionClosedEvents;

    private List<ConnectionEvent> connectionErrorEvents;

    TestingConnectionListener() {
      connectionClosedEvents = new ArrayList<>();
      connectionErrorEvents = new ArrayList<>();
    }

    @Override
    public void connectionClosed(ConnectionEvent event) {
      connectionClosedEvents.add(event);
    }

    @Override
    public void connectionErrorOccurred(ConnectionEvent event) {
      connectionErrorEvents.add(event);
    }

    public List<ConnectionEvent> getConnectionClosedEvents() {
      return connectionClosedEvents;
    }

    public List<ConnectionEvent> getConnectionErrorEvents() {
      return connectionErrorEvents;
    }
  }
}
