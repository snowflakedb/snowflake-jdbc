/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.SQLException;
import java.util.List;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.SFBaseFileTransferAgent.CommandType;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

/**
 * Fixed view result set. This class iterates through any fixed view implementation and return the
 * objects as rows
 */
public class SFFixedViewResultSet extends SFJsonResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFFixedViewResultSet.class);

  private SnowflakeFixedView fixedView;
  private Object[] nextRow = null;
  private final CommandType commandType;

  public SFFixedViewResultSet(SnowflakeFixedView fixedView, CommandType commandType)
      throws SnowflakeSQLException {
    this.fixedView = fixedView;
    this.commandType = commandType;

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
    logger.debug("next called");

    List<Object> nextRowList;
    try {
      // call the fixed view next row method
      nextRowList = fixedView.getNextRow();
    } catch (Exception ex) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(
                  ErrorCode.INTERNAL_ERROR,
                  IncidentUtil.oneLiner("Error getting next row from " + "fixed view:", ex)),
              null,
              null);
    }

    row++;

    if (nextRowList == null) {
      logger.debug("end of result");
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
    logger.debug("public Object getObjectInternal(int columnIndex)");

    if (nextRow == null) {
      throw new SFException(ErrorCode.ROW_DOES_NOT_EXIST);
    }

    if (columnIndex <= 0 || columnIndex > nextRow.length) {
      throw new SFException(ErrorCode.COLUMN_DOES_NOT_EXIST, columnIndex);
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
    return "";
  }
}
