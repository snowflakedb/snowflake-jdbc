package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
public class ConnectionFeatureNotSupportedIT extends BaseJDBCTest {
  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    try (Connection connection = getConnection()) {
      expectFeatureNotSupportedException(() -> connection.rollback(new FakeSavepoint()));
      expectFeatureNotSupportedException(
          () -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
      expectFeatureNotSupportedException(
          () -> connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
      expectFeatureNotSupportedException(
          () -> connection.prepareStatement("select 1", new int[] {1, 2}));
      expectFeatureNotSupportedException(
          () -> connection.prepareStatement("select 1", new String[] {"c1", "c2"}));
      expectFeatureNotSupportedException(
          () ->
              connection.prepareStatement(
                  "select 1", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
      expectFeatureNotSupportedException(
          () ->
              connection.createStatement(
                  ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
      expectFeatureNotSupportedException(() -> connection.setTypeMap(new HashMap<>()));
      expectFeatureNotSupportedException(connection::setSavepoint);
      expectFeatureNotSupportedException(() -> connection.setSavepoint("fake"));
      expectFeatureNotSupportedException(() -> connection.releaseSavepoint(new FakeSavepoint()));
      expectFeatureNotSupportedException(connection::createBlob);
      expectFeatureNotSupportedException(connection::createNClob);
      expectFeatureNotSupportedException(connection::createSQLXML);
      expectFeatureNotSupportedException(
          () -> connection.createStruct("fakeType", new Object[] {}));
      expectFeatureNotSupportedException(
          () -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }
  }

  class FakeSavepoint implements Savepoint {
    @Override
    public int getSavepointId() throws SQLException {
      return 0;
    }

    @Override
    public String getSavepointName() throws SQLException {
      return "";
    }
  }
}
