package net.snowflake.client.api.resultset;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** This interface defines Snowflake specific APIs for ResultSet */
public interface SnowflakeResultSet {
  /**
   * @return the Snowflake query ID of the query which generated this result set
   * @throws SQLException if an error is encountered
   */
  String getQueryID() throws SQLException;

  /**
   * Get a list of ResultSetSerializables for the ResultSet in order to parallel processing
   *
   * @param maxSizeInBytes The expected max data size wrapped in the ResultSetSerializables object.
   *     NOTE: this parameter is intended to make the data size in each serializable object to be
   *     less than it. But if user specifies a small value which may be smaller than the data size
   *     of one result chunk. So the definition can't be guaranteed completely. For this special
   *     case, one serializable object is used to wrap the data chunk.
   * @return a list of ResultSetSerializables
   * @throws SQLException if fails to get the ResultSetSerializable objects.
   */
  List<SnowflakeResultSetSerializable> getResultSetSerializables(long maxSizeInBytes)
      throws SQLException;

  /**
   * Get an array of elements from a structured type (ARRAY) column.
   *
   * <p>This method is used to retrieve array elements with proper type conversion for Snowflake
   * structured types.
   *
   * @param <T> the type of array elements
   * @param columnIndex the column index (1-based)
   * @param type the class of array elements
   * @return an array of elements, or null if the value was SQL NULL
   * @throws SQLException if the column is not a structured type or conversion fails
   */
  <T> T[] getArray(int columnIndex, Class<T> type) throws SQLException;

  /**
   * Get a list of elements from a structured type (ARRAY) column.
   *
   * <p>This method is used to retrieve array elements as a List with proper type conversion for
   * Snowflake structured types.
   *
   * @param <T> the type of list elements
   * @param columnIndex the column index (1-based)
   * @param type the class of list elements
   * @return a List of elements, or null if the value was SQL NULL
   * @throws SQLException if the column is not a structured type or conversion fails
   */
  <T> List<T> getList(int columnIndex, Class<T> type) throws SQLException;

  /**
   * Get a map of key-value pairs from a structured type (MAP or OBJECT) column.
   *
   * <p>This method is used to retrieve map entries with proper type conversion for Snowflake
   * structured types.
   *
   * @param <T> the type of map values
   * @param columnIndex the column index (1-based)
   * @param type the class of map values
   * @return a Map of String keys to typed values, or null if the value was SQL NULL
   * @throws SQLException if the column is not a structured type or conversion fails
   */
  <T> Map<String, T> getMap(int columnIndex, Class<T> type) throws SQLException;
}
