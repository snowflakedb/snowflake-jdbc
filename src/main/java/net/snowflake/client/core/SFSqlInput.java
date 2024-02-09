package net.snowflake.client.core;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInput;
import java.util.TimeZone;

/** This interface extends the standard {@link SQLInput} interface to provide additional methods. */
public interface SFSqlInput extends SQLInput {
  static SFSqlInput unwrap(SQLInput sqlInput) {
    return (SFSqlInput) sqlInput;
  }

  /**
   * Reads the next attribute in the stream and returns it as a <code>java.sql.Timestamp</code>
   * object.
   *
   * @param tz timezone to consider.
   * @return the attribute; if the value is SQL <code>NULL</code>, returns <code>null</code>
   * @exception SQLException if a database access error occurs
   * @exception SQLFeatureNotSupportedException if the JDBC driver does not support this method
   * @since 1.2
   */
  java.sql.Timestamp readTimestamp(TimeZone tz) throws SQLException;
}
