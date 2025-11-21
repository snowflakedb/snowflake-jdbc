package net.snowflake.client.api.connection;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Snowflake-specific extension of {@link DatabaseMetaData}.
 *
 * <p>This interface extends the standard JDBC DatabaseMetaData interface with Snowflake-specific
 * metadata operations.
 */
public interface SnowflakeDatabaseMetaData extends DatabaseMetaData {

  /**
   * Retrieves a description of the streams available in the given catalog.
   *
   * <p>This is a Snowflake-specific extension for retrieving information about Snowflake streams.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; null means that the catalog name should not be used to
   *     narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; null means that the schema name should not
   *     be used to narrow the search
   * @param streamName a stream name pattern; must match the stream name as it is stored in the
   *     database
   * @return a ResultSet object in which each row is a stream description
   * @throws SQLException if a database access error occurs
   */
  ResultSet getStreams(String catalog, String schemaPattern, String streamName) throws SQLException;

  /**
   * Retrieves a description of the table columns available in the specified catalog with extended
   * metadata.
   *
   * <p>This is a Snowflake-specific overload of {@link DatabaseMetaData#getColumns} that allows
   * retrieving extended column metadata.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; null means that the catalog name should not be used to
   *     narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; null means that the schema name should not
   *     be used to narrow the search
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
   *     database
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in
   *     the database
   * @param extendedSet if true, returns extended metadata including base type information
   * @return ResultSet - each row is a column description
   * @throws SQLException if a database access error occurs
   */
  ResultSet getColumns(
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String columnNamePattern,
      boolean extendedSet)
      throws SQLException;
}
