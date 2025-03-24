package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Connection pool interface test */
@Tag(TestTags.CONNECTION)
public class ConnectionPoolingIT {
  private BasicDataSource bds = null;
  private ComboPooledDataSource cpds = null;
  private final String connectStr;
  private final String account;
  private final String user;
  private final String password;
  private final String database;
  private final String schema;
  private final String ssl;

  public ConnectionPoolingIT() {
    Map<String, String> params = BaseJDBCTest.getConnectionParameters();
    connectStr = params.get("uri");
    account = params.get("account");
    user = params.get("user");
    password = params.get("password");
    database = params.get("database");
    schema = params.get("schema");
    ssl = params.get("ssl");
  }

  @BeforeEach
  public void setUp() throws SQLException {
    try (Connection connection = BaseJDBCTest.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create or replace table test_pooling(colA string)");
      statement.execute("insert into test_pooling values('test_str')");
    }
  }

  @AfterEach
  public void tearDown() throws SQLException {
    try (Connection connection = BaseJDBCTest.getConnection();
        Statement statement = connection.createStatement(); ) {
      statement.execute("drop table if exists TEST_POOLING");
    }
  }

  /*  Testing DBCP Library */
  private void setUpDBCPConnection() {
    bds = new BasicDataSource();
    bds.setDriverClassName(BaseJDBCTest.DRIVER_CLASS);
    bds.setUsername(this.user);
    bds.setPassword(this.password);
    bds.setUrl(connectStr);

    String propertyString =
        String.format(
            "ssl=%s;db=%s;schema=%s;account=%s;",
            this.ssl, this.database, this.schema, this.account);
    bds.setConnectionProperties(propertyString);
  }

  @Test
  public void testDBCPConnection() throws SQLException, Exception {
    setUpDBCPConnection();
    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(new queryUsingPool(bds));
      t.join();
    }
  }

  private GenericObjectPool connectionPool = null;

  private PoolingDataSource setUpPoolingDataSource() throws Exception {
    Class.forName(BaseJDBCTest.DRIVER_CLASS);
    connectionPool = new GenericObjectPool();
    connectionPool.setMaxActive(20);
    return new PoolingDataSource(connectionPool);
  }

  public GenericObjectPool getConnectionPool() {
    return connectionPool;
  }

  @Test
  public void testPoolingDataSource() throws Exception, SQLException {
    PoolingDataSource pds = setUpPoolingDataSource();
    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(new queryUsingPool(pds));
      t.join();
    }
  }

  /* Test C3P0 Library */
  private void setUpC3P0Connection() throws PropertyVetoException, SQLException {
    Properties connectionProperties = setUpConnectionProperty();
    cpds = new ComboPooledDataSource();
    cpds.setDriverClass(BaseJDBCTest.DRIVER_CLASS);
    cpds.setJdbcUrl(connectStr);
    cpds.setProperties(connectionProperties);
  }

  @Test
  public void testC3P0ConnectionPool() throws Exception {
    setUpC3P0Connection();

    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(new queryUsingPool(cpds));
      t.join();
    }
  }

  @Test
  public void testHikariConnectionPool() throws Exception {
    HikariDataSource ds = initHikariDataSource();
    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(new queryUsingPool(ds));
      t.join();
    }
  }

  private HikariDataSource initHikariDataSource() throws SQLException {
    HikariConfig config = new HikariConfig();
    config.setDriverClassName(BaseJDBCTest.DRIVER_CLASS);
    config.setDataSourceProperties(setUpConnectionProperty());
    config.setJdbcUrl(connectStr);
    return new HikariDataSource(config);
  }

  private static class queryUsingPool implements Runnable {
    ComboPooledDataSource cpds = null;
    BasicDataSource bds = null;
    PoolingDataSource pds = null;
    HikariDataSource hds = null;

    queryUsingPool(ComboPooledDataSource cpds) {
      this.cpds = cpds;
    }

    queryUsingPool(BasicDataSource bds) {
      this.bds = bds;
    }

    queryUsingPool(PoolingDataSource pds) {
      this.pds = pds;
    }

    queryUsingPool(HikariDataSource hds) {
      this.hds = hds;
    }

    @Override
    public void run() {
      Connection con = null;
      try {
        if (cpds != null) {
          con = cpds.getConnection();
        } else if (bds != null) {
          con = bds.getConnection();
        } else if (pds != null) {
          con = pds.getConnection();
        } else if (hds != null) {
          con = hds.getConnection();
        }

        try (Statement st = con.createStatement();
            ResultSet resultSet = st.executeQuery("SELECT * FROM test_pooling")) {
          while (resultSet.next()) {
            assertEquals("test_str", resultSet.getString(1));
          }
        }
      } catch (Exception e) {
        System.out.println(e);
      }
    }
  }

  private Properties setUpConnectionProperty() {
    Properties properties = new Properties();
    // use the default connection string if it is not set in environment
    properties.put("user", this.user);
    properties.put("password", this.password);
    properties.put("account", this.account);
    properties.put("db", this.database);
    properties.put("schema", this.schema);
    properties.put("ssl", this.ssl);

    return properties;
  }
}
