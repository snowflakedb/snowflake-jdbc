/*
 * Copyright (c) 2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.pooling;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import javax.sql.PooledConnection;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeDriver;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryConnection.class)
public class LogicalConnectionLatestIT extends BaseJDBCTest {
  Map<String, String> properties = getConnectionParameters();

  @Test
  public void testLogicalConnection() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    Connection logicalConnection = pooledConnection.getConnection();
    try (Statement statement =
        logicalConnection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      try (ResultSet resultSet = statement.executeQuery("show parameters")) {
        assertTrue(resultSet.next());
        assertFalse(logicalConnection.isClosed());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, logicalConnection.getHoldability());
      }
    }
    logicalConnection.close();
    assertTrue(logicalConnection.isClosed());
    pooledConnection.close();
  }

  @Test
  public void testNetworkTimeout() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      int millis = logicalConnection.getNetworkTimeout();
      assertEquals(0, millis);
      logicalConnection.setNetworkTimeout(null, 200);
      assertEquals(200, logicalConnection.getNetworkTimeout());
    }
    pooledConnection.close();
  }

  @Test
  public void testIsValid() throws Throwable {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();

    try (Connection logicalConnection = pooledConnection.getConnection()) {
      assertTrue(logicalConnection.isValid(10));
      try {
        assertTrue(logicalConnection.isValid(-10));
        fail("must fail");
      } catch (SQLException ex) {
        // nop, no specific error code is provided.
      }
    }
    pooledConnection.close();
  }

  @Test
  public void testConnectionClientInfo() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      Properties property = logicalConnection.getClientInfo();
      assertEquals(0, property.size());
      Properties clientInfo = new Properties();
      clientInfo.setProperty("name", "Peter");
      clientInfo.setProperty("description", "SNOWFLAKE JDBC");

      expectSQLClientInfoException(() -> logicalConnection.setClientInfo(clientInfo));
      expectSQLClientInfoException(
          () -> logicalConnection.setClientInfo("ApplicationName", "valueA"));
      assertNull(logicalConnection.getClientInfo("Peter"));
    }
    pooledConnection.close();
  }

  @Test
  public void testAbort() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    Connection logicalConnection = pooledConnection.getConnection();
    Connection physicalConnection =
        ((SnowflakePooledConnection) pooledConnection).getPhysicalConnection();
    assertTrue(!physicalConnection.isClosed());
    logicalConnection.abort(null);
    assertTrue(physicalConnection.isClosed());
  }

  @Test
  public void testNativeSQL() throws Throwable {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      // today returning the source SQL.
      assertEquals("select 1", logicalConnection.nativeSQL("select 1"));
    }
    pooledConnection.close();
  }

  @Test
  public void testUnwrapper() throws Throwable {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      boolean canUnwrap = logicalConnection.isWrapperFor(SnowflakeConnectionV1.class);
      assertTrue(canUnwrap);
      if (canUnwrap) {
        SnowflakeConnectionV1 sfconnection = logicalConnection.unwrap(SnowflakeConnectionV1.class);
        sfconnection.createStatement();
      } else {
        fail("should be able to unwrap");
      }
      try {
        logicalConnection.unwrap(SnowflakeDriver.class);
        fail("should fail to cast");
      } catch (SQLException ex) {
        // nop
      }
    }
  }

  @Test
  public void testTransactionStatement() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      logicalConnection.setAutoCommit(false);
      assert (!logicalConnection.getAutoCommit());
      logicalConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      assertEquals(2, logicalConnection.getTransactionIsolation());

      try (Statement statement = logicalConnection.createStatement()) {
        statement.executeUpdate("create or replace table test_transaction (colA int, colB string)");
        // start a transaction
        statement.executeUpdate("insert into test_transaction values (1, 'abc')");

        // commit
        logicalConnection.commit();
        try (ResultSet resultSet =
            statement.executeQuery("select count(*) from test_transaction")) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
        }

        // rollback
        statement.executeUpdate("delete from test_transaction");
        logicalConnection.rollback();
        try (ResultSet resultSet =
            statement.executeQuery("select count(*) from test_transaction")) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
        }
      } finally {
        try (Statement statement = logicalConnection.createStatement()) {
          statement.execute("drop table if exists test_transaction");
        }
      }
    }
    pooledConnection.close();
  }

  @Test
  public void testReadOnly() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      // read only is not supported - will always be false
      assertEquals(false, logicalConnection.isReadOnly());
      logicalConnection.setReadOnly(true);
      assertEquals(false, logicalConnection.isReadOnly());
    }
    pooledConnection.close();
  }

  @Test
  public void testGetTypeMap() throws Throwable {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      // return an empty type map. setTypeMap is not supported.
      assertEquals(Collections.emptyMap(), logicalConnection.getTypeMap());
    }
    pooledConnection.close();
  }

  @Test
  public void testPreparedStatement() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      try (Statement statement = logicalConnection.createStatement()) {
        statement.execute("create or replace table test_prep (colA int, colB varchar)");
        try (PreparedStatement preparedStatement =
            logicalConnection.prepareStatement("insert into test_prep values (?, ?)")) {
          preparedStatement.setInt(1, 25);
          preparedStatement.setString(2, "hello world");
          preparedStatement.execute();
          int count = 0;
          try (ResultSet resultSet = statement.executeQuery("select * from test_prep")) {
            while (resultSet.next()) {
              count++;
            }
          }
          assertEquals(1, count);
        } finally {
          statement.execute("drop table if exists test_prep");
        }
      }
    }
    pooledConnection.close();
  }

  @Test
  public void testSetSchema() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      String schema = logicalConnection.getSchema();
      // get the current schema
      try (ResultSet rst =
          logicalConnection.createStatement().executeQuery("select current_schema()")) {
        assertTrue(rst.next());
        assertEquals(schema, rst.getString(1));
      }

      logicalConnection.setSchema("PUBLIC");

      // get the current schema
      try (ResultSet rst =
          logicalConnection.createStatement().executeQuery("select current_schema()")) {
        assertTrue(rst.next());
        assertEquals("PUBLIC", rst.getString(1));
      }
    }
    pooledConnection.close();
  }

  @Test
  public void testPrepareCall() throws SQLException {
    String procedure =
        "CREATE OR REPLACE PROCEDURE output_message(message VARCHAR)\n"
            + "RETURNS VARCHAR NOT NULL\n"
            + "LANGUAGE SQL\n"
            + "AS\n"
            + "BEGIN\n"
            + "  RETURN message;\n"
            + "END;";
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      try (Statement statement = logicalConnection.createStatement()) {
        statement.execute(procedure);

        try (CallableStatement callableStatement =
            logicalConnection.prepareCall("call output_message(?)")) {
          callableStatement.setString(1, "hello world");
          try (ResultSet resultSet = callableStatement.executeQuery()) {
            resultSet.next();
            assertEquals("hello world", resultSet.getString(1));
          }
        }

        try (CallableStatement callableStatement =
            logicalConnection.prepareCall(
                "call output_message('hello world')",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {
          try (ResultSet resultSet = callableStatement.executeQuery()) {
            resultSet.next();
            assertEquals("hello world", resultSet.getString(1));
            assertEquals(1003, callableStatement.getResultSetType());
            assertEquals(1007, callableStatement.getResultSetConcurrency());
          }
        }

        try (CallableStatement callableStatement =
            logicalConnection.prepareCall(
                "call output_message('hello world')",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
          try (ResultSet resultSet = callableStatement.executeQuery()) {
            resultSet.next();
            assertEquals(2, callableStatement.getResultSetHoldability());
          }
        }
        statement.execute("drop procedure if exists output_message(varchar)");
      }
    }
  }

  @Test
  public void testClob() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      try (Statement statement = logicalConnection.createStatement()) {
        statement.execute("create or replace table test_clob (colA text)");
      }

      try (PreparedStatement preparedStatement =
          logicalConnection.prepareStatement("insert into test_clob values (?)")) {
        Clob clob = logicalConnection.createClob();
        clob.setString(1, "hello world");
        preparedStatement.setClob(1, clob);
        preparedStatement.execute();
      }

      try (Statement statement = logicalConnection.createStatement()) {
        statement.execute("select * from test_clob");
        try (ResultSet resultSet = statement.getResultSet()) {
          resultSet.next();
          assertEquals("hello world", resultSet.getString("COLA"));
        }
      }
    }
  }

  @Test
  public void testDatabaseMetaData() throws SQLException {
    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();
    poolDataSource = setProperties(poolDataSource);
    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    try (Connection logicalConnection = pooledConnection.getConnection()) {
      DatabaseMetaData databaseMetaData = logicalConnection.getMetaData();
      assertEquals("Snowflake", databaseMetaData.getDatabaseProductName());
      assertEquals(properties.get("user"), databaseMetaData.getUserName());
    }
  }

  private SnowflakeConnectionPoolDataSource setProperties(
      SnowflakeConnectionPoolDataSource poolDataSource) {
    poolDataSource.setUrl(properties.get("uri"));
    poolDataSource.setPortNumber(Integer.parseInt(properties.get("port")));
    poolDataSource.setSsl("on".equals(properties.get("ssl")));
    poolDataSource.setAccount(properties.get("account"));
    poolDataSource.setUser(properties.get("user"));
    poolDataSource.setPassword(properties.get("password"));
    poolDataSource.setDatabaseName(properties.get("database"));
    poolDataSource.setSchema(properties.get("schema"));
    poolDataSource.setWarehouse(properties.get("warehouse"));
    return poolDataSource;
  }
}
