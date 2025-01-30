package net.snowflake.client.jdbc;

import static net.snowflake.client.TestUtil.randomIntList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.STATEMENT)
public class BatchExecutionIT extends BaseJDBCTest {
  @Test
  public void testClearingBatchAfterStatementExecution() throws SQLException {
    String tableName = TestUtil.randomTableName("SNOW-1853752");
    int itemsInBatch = 3;
    try (Connection connection = getConnection();
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
        // default behaviour - batch is not cleared
        assertEquals(itemsInBatch, executeMethod.call());
        statement.clearBatch();
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

  /**
   * ThrowingCallable is defined here since is not available in OldDriverTests from main package.
   * Can be removed when OldDriver version is set to >=3.15.1
   */
  @FunctionalInterface
  interface ThrowingCallable<A, T extends Throwable> {
    A call() throws T;
  }
}
