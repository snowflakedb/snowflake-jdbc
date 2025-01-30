package net.snowflake.client.jdbc;

import static net.snowflake.client.TestUtil.randomIntList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.util.ThrowingCallable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.STATEMENT)
public class BatchExecutionLatestIT extends BaseJDBCTest {
  /** Available after version 3.21.0 */
  @Test
  public void testClearingBatchAfterStatementExecution() throws SQLException {
    String tableName = TestUtil.randomTableName("SNOW-1853752");
    int itemsInBatch = 3;
    Properties properties = new Properties();
    properties.put("CLEAR_BATCH_ONLY_AFTER_SUCCESSFUL_EXECUTION", true);
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("CREATE OR REPLACE TABLE %s(id int, s varchar(2))", tableName));
      List<ThrowingCallable<Integer, SQLException>> executeMethods =
          Arrays.asList(
              () -> statement.executeBatch().length, () -> statement.executeLargeBatch().length);
      for (ThrowingCallable<Integer, SQLException> executeMethod : executeMethods) {
        for (int i : randomIntList(itemsInBatch, 10)) {
          statement.addBatch(
              String.format("INSERT INTO %s(id, s) VALUES (%d, 's%d')", tableName, i, i));
        }
        assertEquals(itemsInBatch, executeMethod.call());
        assertEquals(0, executeMethod.call());
        for (int i : randomIntList(itemsInBatch, 10)) {
          statement.addBatch(
              String.format(
                  "INSERT INTO %s(id, s) VALUES (%d, 'longer string %d')", tableName, i, i));
        }
        assertThrows(BatchUpdateException.class, executeMethod::call);
        // second call should also fail since batch should not be cleared
        assertThrows(BatchUpdateException.class, executeMethod::call);
        // clear batch for next execution in loop
        statement.clearBatch();
      }
    }
  }

  /** Available after version 3.21.0 */
  @Test
  public void testClearingBatchAfterPreparedStatementExecutionWithArrayBinding()
      throws SQLException {
    String tableName = TestUtil.randomTableName("SNOW-1853752");
    int itemsInBatch = 3;
    Properties properties = new Properties();
    properties.put("CLEAR_BATCH_ONLY_AFTER_SUCCESSFUL_EXECUTION", true);
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("CREATE OR REPLACE TABLE %s(id int, s varchar(2))", tableName));
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(
              String.format("INSERT INTO %s(id, s) VALUES (?, ?)", tableName))) {
        List<ThrowingCallable<Integer, SQLException>> executeMethods =
            Arrays.asList(
                () -> preparedStatement.executeBatch().length,
                () -> preparedStatement.executeLargeBatch().length);
        for (ThrowingCallable<Integer, SQLException> executeMethod : executeMethods) {
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "s" + i);
            preparedStatement.addBatch();
          }
          assertEquals(itemsInBatch, executeMethod.call());
          assertEquals(0, executeMethod.call());
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i * 10);
            preparedStatement.setString(2, "longer string " + i);
            preparedStatement.addBatch();
          }
          // With array binding there is SnowflakeSQLException thrown but BatchUpdateException is
          // more expected - should we change?
          assertThrows(SnowflakeSQLException.class, executeMethod::call);
          // second call should also fail since batch should not be cleared
          assertThrows(SnowflakeSQLException.class, executeMethod::call);
          // clear batch for next execution in loop
          preparedStatement.clearBatch();
        }
      }
    }
  }

  /** Available after version 3.21.0 */
  @Test
  @Disabled("TODO SNOW-1878297 NULLIF should cause turning off array binding on server side")
  public void testClearingBatchAfterPreparedStatementExecutionWithoutArrayBinding()
      throws SQLException {
    String tableName = TestUtil.randomTableName("SNOW-1853752");
    int itemsInBatch = 3;
    Properties properties = new Properties();
    properties.put("CLEAR_BATCH_ONLY_AFTER_SUCCESSFUL_EXECUTION", true);
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("CREATE OR REPLACE TABLE %s(id int, s varchar(2))", tableName));
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(
              // NULLIF prohibit array binding and forces calling sqls one by one
              String.format("INSERT INTO %s(id, s) VALUES (?, NULLIF(?, ''))", tableName))) {
        List<ThrowingCallable<Integer, SQLException>> executeMethods =
            Arrays.asList(
                () -> preparedStatement.executeBatch().length,
                () -> preparedStatement.executeLargeBatch().length);
        for (ThrowingCallable<Integer, SQLException> executeMethod : executeMethods) {
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "s" + i);
            preparedStatement.addBatch();
          }
          assertEquals(itemsInBatch, executeMethod.call());
          assertEquals(0, executeMethod.call());
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i * 10);
            preparedStatement.setString(2, "longer string " + i);
            preparedStatement.addBatch();
          }
          assertThrows(BatchUpdateException.class, executeMethod::call);
          // second call should also fail since batch should not be cleared
          assertThrows(BatchUpdateException.class, executeMethod::call);
          // clear batch for next execution in loop
          preparedStatement.clearBatch();
        }
      }
    }
  }

  /** Available after version 3.13.31 when executeLargeBatch was fixed */
  @Test
  public void testClearingBatchAfterPreparedStatementExecutionWithArrayBindingDefaultBehaviour()
      throws SQLException {
    String tableName = TestUtil.randomTableName("SNOW-1853752");
    int itemsInBatch = 3;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("CREATE OR REPLACE TABLE %s(id int, s varchar(2))", tableName));
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(
              String.format("INSERT INTO %s(id, s) VALUES (?, ?)", tableName))) {
        List<BatchExecutionIT.ThrowingCallable<Integer, SQLException>> executeMethods =
            Arrays.asList(
                () -> preparedStatement.executeBatch().length,
                () -> preparedStatement.executeLargeBatch().length);
        for (BatchExecutionIT.ThrowingCallable<Integer, SQLException> executeMethod :
            executeMethods) {
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "s" + i);
            preparedStatement.addBatch();
          }
          assertEquals(itemsInBatch, executeMethod.call());
          assertEquals(0, executeMethod.call());
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i * 10);
            preparedStatement.setString(2, "longer string " + i);
            preparedStatement.addBatch();
          }
          // With array binding there is SnowflakeSQLException thrown but BatchUpdateException is
          // more expected - should we change?
          assertThrows(SnowflakeSQLException.class, executeMethod::call);
          // default behaviour - prepared statement batch is always cleared after execution
          assertEquals(0, executeMethod.call());
          // clear batch for next execution in loop
          preparedStatement.clearBatch();
        }
      }
    }
  }

  /** Available after version 3.13.31 when executeLargeBatch was fixed */
  @Test
  @Disabled("TODO SNOW-1878297 NULLIF should cause turning off array binding on server side")
  public void testClearingBatchAfterPreparedStatementExecutionWithoutArrayBindingDefaultBehaviour()
      throws SQLException {
    String tableName = TestUtil.randomTableName("SNOW-1853752");
    int itemsInBatch = 3;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format("CREATE OR REPLACE TABLE %s(id int, s varchar(2))", tableName));
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(
              // NULLIF prohibit array binding and forces calling sqls one by one
              String.format("INSERT INTO %s(id, s) VALUES (?, NULLIF(?, ''))", tableName))) {
        List<BatchExecutionIT.ThrowingCallable<Integer, SQLException>> executeMethods =
            Arrays.asList(
                () -> preparedStatement.executeBatch().length,
                () -> preparedStatement.executeLargeBatch().length);
        for (BatchExecutionIT.ThrowingCallable<Integer, SQLException> executeMethod :
            executeMethods) {
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "s" + i);
            preparedStatement.addBatch();
          }
          assertEquals(itemsInBatch, executeMethod.call());
          assertEquals(0, executeMethod.call());
          for (int i : randomIntList(itemsInBatch, 10)) {
            preparedStatement.setInt(1, i * 10);
            preparedStatement.setString(2, "longer string " + i);
            preparedStatement.addBatch();
          }
          assertThrows(BatchUpdateException.class, executeMethod::call);
          // default behaviour - prepared statement batch is always cleared after execution
          assertEquals(0, executeMethod.call());
          // clear batch for next execution in loop
          preparedStatement.clearBatch();
        }
      }
    }
  }
}
