/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.SqlState;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.ResultUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Snowflake ResultSetMetaData
 *
 * @author jhuang
 */
public class SnowflakeResultSetMetaData implements ResultSetMetaData
{

  static final
  SFLogger logger = SFLoggerFactory.getLogger(SnowflakeResultSetMetaData.class);

  private int columnCount = 0;

  private List<String> columnNames;

  private List<String> columnTypeNames;

  private List<Integer> columnTypes;

  private List<Integer> precisions;

  private List<Integer> scales;

  private String queryId;

  private Map<String, Integer> columnNamePositionMap =
  new HashMap<>();

  private Map<String, Integer> columnNameUpperCasePositionMap =
      new HashMap<>();

  private SFSession session;

  public SnowflakeResultSetMetaData() {}

  public SnowflakeResultSetMetaData(int columnCount,
                                    List<String> columnNames,
                                    List<String> columnTypeNames,
                                    List<Integer> columnTypes,
                                    SFSession session)
          throws SnowflakeSQLException
  {
    this.columnCount = columnCount;
    this.columnNames = columnNames;
    this.columnTypeNames = columnTypeNames;
    this.columnTypes = columnTypes;
    this.session = session;
  }

  /**
   * @return query id
   */
  public String getQueryId()
  {
    return queryId;
  }

  /**
   * @return list of column names
   */
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  /**
   * @param columnName column name
   * @return index of the column
   */
  public int getColumnIndex(String columnName)
  {
    boolean caseInsensitive = session != null && session.isResultColumnCaseInsensitive();
    columnName = caseInsensitive ? columnName.toUpperCase() : columnName;
    Map<String, Integer> nameToIndexMap = caseInsensitive ?
        columnNameUpperCasePositionMap : columnNamePositionMap;

    if (nameToIndexMap.get(columnName) != null)
    {
      return nameToIndexMap.get(columnName);
    }
    else
    {
      int columnIndex = caseInsensitive ?
          ResultUtil.listSearchCaseInsensitive(columnNames, columnName) :
          columnNames.indexOf(columnName);
      nameToIndexMap.put(columnName, columnIndex);
      return columnIndex;
    }
  }

  /**
   * @return column count
   * @throws SQLException if failed to get column count
   */
  @Override
  public int getColumnCount() throws SQLException
  {
    logger.debug(
	       "public int getColumnCount(), columnCount= {}",
	       columnCount);

    return columnCount;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException
  {
    logger.debug("public boolean isAutoIncrement(int column)");

    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException
  {
    logger.debug("public boolean isCaseSensitive(int column)");

    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException
  {
    logger.debug("public boolean isSearchable(int column)");

    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException
  {
    logger.debug("public boolean isCurrency(int column)");

    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException
  {
    logger.debug("public int isNullable(int column)");

    return columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException
  {
    logger.debug("public boolean isSigned(int column)");

    if (columnTypes.get(column-1) == Types.INTEGER ||
        columnTypes.get(column-1) == Types.DECIMAL ||
        columnTypes.get(column-1) == Types.DOUBLE)
      return true;
    else
      return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException
  {
    logger.debug("public int getColumnDisplaySize(int column)");

    return 25;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException
  {
    logger.debug("public String getColumnLabel(int column)");

    if(columnNames != null)
      return columnNames.get(column-1);
    else
      return "C" + Integer.toString(column-1);
  }

  @Override
  public String getColumnName(int column) throws SQLException
  {
    logger.debug("public String getColumnName(int column)");

    if(columnNames != null)
      return columnNames.get(column-1);
    else
      return "C" + Integer.toString(column-1);
  }

  @Override
  public String getSchemaName(int column) throws SQLException
  {
    logger.debug("public String getSchemaName(int column)");

    return "";
  }

  @Override
  public int getPrecision(int column) throws SQLException
  {
    logger.debug("public int getPrecision(int column)");

    if (precisions != null && precisions.size() >= column)
    {
      return precisions.get(column-1);
    }
    else
    {
      // TODO: fix this later to use different defaults for number or timestamp
      return 9;
    }
  }

  @Override
  public int getScale(int column) throws SQLException
  {
    logger.debug("public int getScale(int column)");

    if (scales != null && scales.size() >= column)
    {
      return scales.get(column-1);
    }
    else
    {
      // TODO: fix this later to use different defaults for number or timestamp
      return 9;
    }
  }

  @Override
  public String getTableName(int column) throws SQLException
  {
    logger.debug("public String getTableName(int column)");

    return "T";
  }

  @Override
  public String getCatalogName(int column) throws SQLException
  {
    logger.debug("public String getCatalogName(int column)");

    return "";
  }

  @Override
  public int getColumnType(int column) throws SQLException
  {
    logger.debug("public int getColumnType(int column)");

    int internalColumnType = getInternalColumnType(column);

    int externalColumnType = internalColumnType;

    if (internalColumnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ ||
        internalColumnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ)
      externalColumnType = Types.TIMESTAMP;

    logger.debug(
	       "column type = {}", 
	       externalColumnType);

    return externalColumnType;
  }

  public int getInternalColumnType(int column) throws SQLException
  {
    logger.debug("public int getInternalColumnType(int column)");

    int columnIdx = column-1;
    if (column > columnTypes.size())
    {
      throw new SQLException("Invalid column index: " + column);
    }

    if(columnTypes.get(columnIdx) == null)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "Missing column type for column " + column);
    }

    logger.debug(
	       "column type = {}",
	       columnTypes.get(column-1));

    return columnTypes.get(columnIdx);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException
  {
    logger.debug("public String getColumnTypeName(int column)");

    if (column > columnTypeNames.size())
    {
      throw new SQLException("Invalid column index: " + column);
    }

    if(columnTypeNames == null || columnTypeNames.get(column-1) == null)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "Missing column type for column " + column);
    }

    return columnTypeNames.get(column-1);
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException
  {
    logger.debug("public boolean isReadOnly(int column)");

    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException
  {
    logger.debug("public boolean isWritable(int column)");

    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException
  {
    logger.debug("public boolean isDefinitelyWritable(int column)");

    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException
  {
    logger.debug("public String getColumnClassName(int column)");

    int type = this.getColumnType(column);

    switch(type)
    {
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.BINARY:
        return String.class.getName();

      case Types.INTEGER:
        return Integer.class.getName();

      case Types.DECIMAL:
        return BigDecimal.class.getName();

      case Types.DOUBLE:
        return Double.class.getName();

      case Types.TIMESTAMP:
        return Timestamp.class.getName();

      case Types.DATE:
        return Date.class.getName();

      case Types.TIME:
        return Time.class.getName();

      case Types.BOOLEAN:
        return Boolean.class.getName();

      case Types.BIGINT:
        return Long.class.getName();

      default:
        throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    logger.debug("public boolean isWrapperFor(Class<?> iface)");

    throw new SQLFeatureNotSupportedException();
  }
}
