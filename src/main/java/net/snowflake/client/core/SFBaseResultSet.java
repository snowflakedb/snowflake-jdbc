/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeChunkDownloader;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Base class for query result set and metadata result set
 */
public abstract class SFBaseResultSet
{
  static private final SFLogger logger =
      SFLoggerFactory.getLogger(SFBaseResultSet.class);

  boolean wasNull = false;

  protected SFResultSetMetaData resultSetMetaData = null;

  protected int row = 0;

  protected Map<String, Object> parameters = new HashMap<>();

  // Formatters for different datatypes
  SnowflakeDateTimeFormat timestampNTZFormatter;
  SnowflakeDateTimeFormat timestampLTZFormatter;
  SnowflakeDateTimeFormat timestampTZFormatter;
  SnowflakeDateTimeFormat dateFormatter;
  SnowflakeDateTimeFormat timeFormatter;
  boolean honorClientTZForTimestampNTZ = true;
  SFBinaryFormat binaryFormatter;

  protected long resultVersion = 0;

  protected int numberOfBinds = 0;

  protected List<MetaDataOfBinds> metaDataOfBinds = new ArrayList<>();

  // For creating incidents
  protected SFSession session;

  // indicate whether the result set has been closed or not.
  protected boolean isClosed;

  abstract public boolean isLast();

  abstract public boolean isAfterLast();

  public abstract String getString(int columnIndex) throws SFException;

  public abstract boolean getBoolean(int columnIndex) throws SFException;

  public abstract byte getByte(int columnIndex) throws SFException;

  public abstract short getShort(int columnIndex) throws SFException;

  public abstract int getInt(int columnIndex) throws SFException;

  public abstract long getLong(int columnIndex) throws SFException;

  public abstract float getFloat(int columnIndex) throws SFException;

  public abstract double getDouble(int columnIndex) throws SFException;

  public abstract byte[] getBytes(int columnIndex) throws SFException;

  public abstract Date getDate(int columnIndex) throws SFException;

  public abstract Time getTime(int columnIndex) throws SFException;

  public abstract Timestamp getTimestamp(int columnIndex, TimeZone tz)
  throws SFException;

  public abstract Object getObject(int columnIndex) throws SFException;

  public abstract BigDecimal getBigDecimal(int columnIndex) throws SFException;

  public abstract BigDecimal getBigDecimal(int columnIndex, int scale) throws SFException;

  public abstract SFStatementType getStatementType();

  // this is useful to override the initial statement type if it is incorrect
  // (e.g. result scan yields a query type, but the results are from a DML statement)
  public abstract void setStatementType(SFStatementType statementType)
  throws SQLException;

  public abstract String getQueryId();

  public void setSession(SFSession session)
  {
    this.session = session;
  }

  // default implementation
  public boolean next() throws SFException, SnowflakeSQLException
  {
    logger.debug("public boolean next()");
    return false;
  }

  public void close()
  {
    logger.debug("public void close()");

    // no exception even if already closed.
    resultSetMetaData = null;
    isClosed = true;
  }

  public boolean wasNull()
  {
    logger.debug("public boolean wasNull() returning {}", wasNull);

    return wasNull;
  }


  public SFResultSetMetaData getMetaData() throws SFException
  {
    return resultSetMetaData;
  }

  public int getRow() throws SQLException
  {
    return row;
  }

  public boolean absolute(int row) throws SFException
  {
    throw new SFException(
        ErrorCode.FEATURE_UNSUPPORTED, "seek to a specific row");
  }

  public boolean relative(int rows) throws SFException
  {
    throw new SFException(
        ErrorCode.FEATURE_UNSUPPORTED, "seek to a row relative to current row");
  }

  public boolean previous() throws SFException
  {
    throw new SFException(
        ErrorCode.FEATURE_UNSUPPORTED, "seek to a previous row");
  }

  protected int getNumberOfBinds()
  {
    return numberOfBinds;
  }

  protected List<MetaDataOfBinds> getMetaDataOfBinds()
  {
    return metaDataOfBinds;
  }

  public boolean isFirst()
  {
    return row == 1;
  }

  public boolean isBeforeFirst()
  {
    return row == 0;
  }

  public boolean isClosed()
  {
    return isClosed;
  }

  public boolean isArrayBindSupported()
  {
    return false;
  }
}
