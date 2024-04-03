/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInput;
import java.util.TimeZone;

/** This interface extends the standard {@link SQLInput} interface to provide additional methods. */
@SnowflakeJdbcInternalApi
public interface SFSqlInput extends SQLInput {

  /**
   * Method unwrapping object of class SQLInput to object of class SfSqlInput.
   *
   * @param sqlInput SQLInput to consider.
   * @return Object unwrapped to SFSqlInput class.
   */
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
  /**
   * Reads the next attribute in the stream and returns it as a <code>Object</code> object.
   *
   * @param <T> the type of the class modeled by this Class object
   * @param type Class representing the Java data type to convert the attribute to.
   * @return the attribute at the head of the stream as an {@code Object} in the Java programming
   *     language;{@code null} if the attribute is SQL {@code NULL}
   * @exception SQLException if a database access error occurs
   * @exception SQLFeatureNotSupportedException if the JDBC driver does not support this method
   * @since 1.8
   */
  <T> T readObject(Class<T> type, TimeZone tz) throws SQLException;
}
