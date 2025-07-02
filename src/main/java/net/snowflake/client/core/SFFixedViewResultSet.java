package net.snowflake.client.core;

import java.sql.SQLException;
import java.util.List;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SFBaseFileTransferAgent;
import net.snowflake.client.jdbc.SFBaseFileTransferAgent.CommandType;
import net.snowflake.client.jdbc.SnowflakeFixedView;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SqlState;

/**
 * Fixed view result set. This class iterates through any fixed view implementation and return the
 * objects as rows
 */
// Works only for strings, numbers, etc, does not work for timestamps, dates, times etc.
public class SFFixedViewResultSet extends SFJsonResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFFixedViewResultSet.class);

  private SnowflakeFixedView fixedView;
  private Object[] nextRow = null;
  private final CommandType commandType;
  private final String queryID;

  public SFFixedViewResultSet(SnowflakeFixedView fixedView, CommandType commandType, String queryID)
      throws SnowflakeSQLException {
    super(
        null,
        new Converters(
            null,
            new SFSession(),
            0,
            false,
            false,
            false,
            false,
            SFBinaryFormat.BASE64,
            null,
            null,
            null,
            null,
            null));
    this.fixedView = fixedView;
    this.commandType = commandType;
    this.queryID = queryID;

    try {
      resultSetMetaData =
          new SFResultSetMetaData(
              fixedView.describeColumns(session),
              session,
              timestampNTZFormatter,
              timestampLTZFormatter,
              timestampTZFormatter,
              dateFormatter,
              timeFormatter);

    } catch (Exception ex) {
      throw new SnowflakeSQLLoggedException(
          queryID,
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to describe fixed view: " + fixedView.getClass().getName());
    }
  }

  /**
   * Advance to next row
   *
   * @return true if next row exists, false otherwise
   * @throws net.snowflake.client.core.SFException if failed to get next row
   */
  @Override
  public boolean next() throws SFException {
    logger.trace("next called", false);

    List<Object> nextRowList;
    try {
      // call the fixed view next row method
      nextRowList = fixedView.getNextRow();
    } catch (Exception ex) {
      throw new SFException(
          queryID,
          ErrorCode.INTERNAL_ERROR,
          IncidentUtil.oneLiner("Error getting next row from " + "fixed view:", ex));
    }

    row++;

    if (nextRowList == null) {
      logger.debug("End of result", false);
      return false;
    }

    if (nextRow == null) {
      nextRow = new Object[nextRowList.size()];
    }
    nextRow = nextRowList.toArray(nextRow);

    return true;
  }

  @Override
  protected Object getObjectInternal(int columnIndex) throws SFException {
    logger.trace("Object getObjectInternal(int columnIndex)", false);

    if (nextRow == null) {
      throw new SFException(queryID, ErrorCode.ROW_DOES_NOT_EXIST);
    }

    if (columnIndex <= 0 || columnIndex > nextRow.length) {
      throw new SFException(queryID, ErrorCode.COLUMN_DOES_NOT_EXIST, columnIndex);
    }

    wasNull = nextRow[columnIndex - 1] == null;

    return nextRow[columnIndex - 1];
  }

  @Override
  public void close() throws SnowflakeSQLException {
    super.close();
    // free the object so that they can be Garbage collected
    nextRow = null;
    fixedView = null;
  }

  @Override
  public SFStatementType getStatementType() {
    if (this.commandType == SFBaseFileTransferAgent.CommandType.DOWNLOAD) {
      return SFStatementType.GET;
    } else {
      return SFStatementType.PUT;
    }
  }

  @Override
  public void setStatementType(SFStatementType statementType) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean isLast() {
    return row == fixedView.getTotalRows();
  }

  @Override
  public boolean isAfterLast() {
    return row > fixedView.getTotalRows();
  }

  @Override
  public String getQueryId() {
    return queryID;
  }
}
