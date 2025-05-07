package net.snowflake.client.core;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Base abstract class for an SFStatement implementation. Statements are used in executing queries,
 * both in standard and prepared forms. They are accessed by users via the public API class,
 * SnowflakeStatementV(x).
 */
public abstract class SFBaseStatement {
  // maximum number of parameters for the statement; if this threshold is exceeded,
  // we throw an exception
  protected static final int MAX_STATEMENT_PARAMETERS = 1000;
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFBaseStatement.class);
  // statement level parameters; just a string-key, object-value map.
  protected final Map<String, Object> statementParametersMap = new HashMap<>();
  // timeout in seconds for queries
  protected int queryTimeout = 0;

  /**
   * Add a statement parameter
   *
   * <p>Make sure a property is not added more than once and the number of properties does not
   * exceed limit.
   *
   * @param propertyName property name
   * @param propertyValue property value
   * @throws SFException if too many parameters for a statement
   */
  public void addProperty(String propertyName, Object propertyValue) throws SFException {
    statementParametersMap.put(propertyName, propertyValue);

    if ("query_timeout".equalsIgnoreCase(propertyName)) {
      queryTimeout = (Integer) propertyValue;
    }

    // check if the number of session properties exceed limit
    if (statementParametersMap.size() > MAX_STATEMENT_PARAMETERS) {
      throw new SFException(ErrorCode.TOO_MANY_STATEMENT_PARAMETERS, MAX_STATEMENT_PARAMETERS);
    }
  }

  public Map<String, Object> getStatementParameters() {
    return statementParametersMap;
  }

  /**
   * Describe a statement. This is invoked when prepareStatement() occurs. SFStatementMetadata
   * should be returned by this action, which contains metadata such as the schema of the result.
   *
   * @param sql The SQL string of the query/statement.
   * @return metadata of statement including resultset metadata and binding information
   * @throws SQLException if connection is already closed
   * @throws SFException if result set is null
   */
  public abstract SFPreparedStatementMetaData describe(String sql) throws SFException, SQLException;

  /**
   * Executes the given SQL string.
   *
   * @param sql The SQL string to execute, synchronously.
   * @param parametersBinding parameters to bind
   * @param caller the JDBC interface method that called this method, if any
   * @param execTimeData OOB telemetry object to record timings
   * @return whether there is result set or not
   * @throws SQLException if failed to execute sql
   * @throws SFException exception raised from Snowflake components
   * @throws SQLException if SQL error occurs
   */
  public abstract SFBaseResultSet execute(
      String sql,
      Map<String, ParameterBindingDTO> parametersBinding,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException;

  /**
   * Execute sql asynchronously. Note that at a minimum, this does not have to be supported; if
   * executeAsyncQuery() is called from SnowflakeStatement and the SFConnectionHandler's
   * supportsAsyncQuery() returns false, an exception is thrown. If this is un-implemented, then
   * supportsAsyncQuery() should return false.
   *
   * @param sql sql statement.
   * @param parametersBinding parameters to bind
   * @param caller the JDBC interface method that called this method, if any
   * @param execTimeData ExecTimeTelemetryData
   * @return whether there is result set or not
   * @throws SQLException if failed to execute sql
   * @throws SFException exception raised from Snowflake components
   * @throws SQLException if SQL error occurs
   */
  public abstract SFBaseResultSet asyncExecute(
      String sql,
      Map<String, ParameterBindingDTO> parametersBinding,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException;

  /**
   * Closes the statement. Open result sets are closed, connections are terminated, state is
   * cleared, etc.
   */
  public abstract void close();

  /**
   * Aborts the statement.
   *
   * @throws SFException if the statement is already closed.
   * @throws SQLException if there are server-side errors from trying to abort.
   * @deprecated use {@link #cancel(CancellationReason)} instead
   */
  @Deprecated
  public abstract void cancel() throws SFException, SQLException;

  /**
   * Aborts the statement.
   *
   * @param cancellationReason reason for the cancellation
   * @throws SFException if the statement is already closed.
   * @throws SQLException if there are server-side errors from trying to abort.
   */
  @SnowflakeJdbcInternalApi
  public void cancel(CancellationReason cancellationReason) throws SFException, SQLException {
    cancel(); // default cancel is called to keep interface backward compatibility
  }

  /**
   * Sets a property within session properties, i.e., if the sql is using set-sf-property
   *
   * @param sql the set property sql
   */
  public void executeSetProperty(final String sql) {
    logger.trace("Setting property", false);

    // tokenize the sql
    String[] tokens = sql.split("\\s+");

    if (tokens.length < 2) {
      return;
    }

    if ("sort".equalsIgnoreCase(tokens[1])) {
      if (tokens.length >= 3 && "on".equalsIgnoreCase(tokens[2])) {
        logger.debug("Setting sort on", false);

        this.getSFBaseSession().setSessionPropertyByKey("sort", true);
      } else {
        logger.debug("Setting sort off", false);
        this.getSFBaseSession().setSessionPropertyByKey("sort", false);
      }
    }
  }

  /**
   * A method to check if a sql is file upload statement with consideration for potential comments
   * in front of put keyword.
   *
   * @param sql sql statement
   * @return true if the command is upload statement
   */
  public static boolean isFileTransfer(String sql) {
    SFStatementType statementType = StmtUtil.checkStageManageCommand(sql);
    return statementType == SFStatementType.PUT || statementType == SFStatementType.GET;
  }

  /**
   * If this is a multi-statement, i.e., has child results.
   *
   * @return true if has child results
   */
  public abstract boolean hasChildren();

  /**
   * Get the SFBaseSession associated with this SFBaseStatement.
   *
   * @return The SFBaseSession associated with this SFBaseStatement.
   */
  public abstract SFBaseSession getSFBaseSession();

  /**
   * Retrieves the current result as a ResultSet, if any. This is invoked by SnowflakeStatement and
   * should return an SFBaseResultSet, which is then wrapped in a SnowflakeResultSet.
   *
   * @return {@link SFBaseResultSet}
   */
  public abstract SFBaseResultSet getResultSet();

  /**
   * Sets the result set to the next one, if available.
   *
   * @param current What to do with the current result. One of Statement.CLOSE_CURRENT_RESULT,
   *     Statement.CLOSE_ALL_RESULTS, or Statement.KEEP_CURRENT_RESULT
   * @return true if there is a next result and it's a result set false if there are no more
   *     results, or there is a next result and it's an update count
   * @throws SQLException if something fails while getting the next result
   */
  public abstract boolean getMoreResults(int current) throws SQLException;

  /** The type of query that is being executed. Used internally by SnowflakeStatementV(x). */
  public enum CallingMethod {
    EXECUTE,
    EXECUTE_UPDATE,
    EXECUTE_QUERY
  }

  public abstract long getConservativeMemoryLimit();

  public abstract int getConservativePrefetchThreads();

  /**
   * @param queryID the queryID
   * @return the child query IDs for the multiple statements query.
   * @throws SQLException if an error occurs while getting child query ID's
   */
  public abstract String[] getChildQueryIds(String queryID) throws SQLException;
}
