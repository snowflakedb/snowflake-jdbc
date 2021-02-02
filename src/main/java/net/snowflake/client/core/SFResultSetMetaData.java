/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.*;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeColumnMetadata;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

/** Snowflake ResultSetMetaData */
public class SFResultSetMetaData {
  static final SFLogger logger = SFLoggerFactory.getLogger(SFResultSetMetaData.class);

  private int columnCount = 0;

  private List<String> columnNames;

  private List<String> columnTypeNames;

  private List<Integer> columnTypes;

  private List<Integer> precisions;

  private List<Integer> scales;

  private List<Integer> nullables;

  private List<String> columnSrcTables;

  private List<String> columnSrcSchemas;

  private List<String> columnSrcDatabases;

  private List<Integer> columnDisplaySizes;

  private String queryId;

  private Map<String, Integer> columnNamePositionMap = new HashMap<>();

  private Map<String, Integer> columnNameUpperCasePositionMap = new HashMap<>();

  // For creating incidents
  private SFBaseSession session;

  // Date time formatter for calculating the display size
  private SnowflakeDateTimeFormat timestampNTZFormatter;

  private SnowflakeDateTimeFormat timestampLTZFormatter;

  private SnowflakeDateTimeFormat timestampTZFormatter;

  private SnowflakeDateTimeFormat timeFormatter;

  private SnowflakeDateTimeFormat dateFormatter;

  // provide default display size for databasemetadata result set.
  // i.e. result set returned calling getTables etc
  private int timestampNTZStringLength = 30;

  private int timestampLTZStringLength = 30;

  private int timestampTZStringLength = 30;

  private int timeStringLength = 18;

  private int dateStringLength = 10;

  private boolean isResultColumnCaseInsensitive = false;

  public SFResultSetMetaData(
      int columnCount,
      List<String> columnNames,
      List<String> columnTypeNames,
      List<Integer> columnTypes,
      SFBaseSession session) {
    this.columnCount = columnCount;
    this.columnNames = columnNames;
    this.columnTypeNames = columnTypeNames;
    this.columnTypes = columnTypes;
    this.session = session;
  }

  public SFResultSetMetaData(
      List<SnowflakeColumnMetadata> columnMetadata,
      SFBaseSession session,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter,
      SnowflakeDateTimeFormat dateFormatter,
      SnowflakeDateTimeFormat timeFormatter) {
    this(
        columnMetadata,
        "none",
        session,
        (session != null) && session.isResultColumnCaseInsensitive(),
        timestampNTZFormatter,
        timestampLTZFormatter,
        timestampTZFormatter,
        dateFormatter,
        timeFormatter);
  }

  public SFResultSetMetaData(
      List<SnowflakeColumnMetadata> columnMetadata,
      String queryId,
      SFBaseSession session,
      boolean isResultColumnCaseInsensitive,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter,
      SnowflakeDateTimeFormat dateFormatter,
      SnowflakeDateTimeFormat timeFormatter) {
    this.columnCount = columnMetadata.size();
    this.queryId = queryId;
    this.timestampNTZFormatter = timestampNTZFormatter;
    this.timestampLTZFormatter = timestampLTZFormatter;
    this.timestampTZFormatter = timestampTZFormatter;
    this.dateFormatter = dateFormatter;
    this.timeFormatter = timeFormatter;
    calculateDateTimeStringLength();

    this.columnNames = new ArrayList<>(this.columnCount);
    this.columnTypeNames = new ArrayList<>(this.columnCount);
    this.columnTypes = new ArrayList<>(this.columnCount);
    this.precisions = new ArrayList<>(this.columnCount);
    this.scales = new ArrayList<>(this.columnCount);
    this.nullables = new ArrayList<>(this.columnCount);
    this.columnSrcDatabases = new ArrayList<>(this.columnCount);
    this.columnSrcSchemas = new ArrayList<>(this.columnCount);
    this.columnSrcTables = new ArrayList<>(this.columnCount);
    this.columnDisplaySizes = new ArrayList<>(this.columnCount);
    this.isResultColumnCaseInsensitive = isResultColumnCaseInsensitive;

    for (int colIdx = 0; colIdx < columnCount; colIdx++) {
      columnNames.add(columnMetadata.get(colIdx).getName());
      columnTypeNames.add(columnMetadata.get(colIdx).getTypeName());
      precisions.add(calculatePrecision(columnMetadata.get(colIdx)));
      columnTypes.add(columnMetadata.get(colIdx).getType());
      scales.add(columnMetadata.get(colIdx).getScale());
      nullables.add(
          columnMetadata.get(colIdx).isNullable()
              ? ResultSetMetaData.columnNullable
              : ResultSetMetaData.columnNoNulls);
      columnSrcDatabases.add(columnMetadata.get(colIdx).getColumnSrcDatabase());
      columnSrcSchemas.add(columnMetadata.get(colIdx).getColumnSrcSchema());
      columnSrcTables.add(columnMetadata.get(colIdx).getColumnSrcTable());
      columnDisplaySizes.add(calculateDisplaySize(columnMetadata.get(colIdx)));
    }

    this.session = session;
  }

  private Integer calculatePrecision(SnowflakeColumnMetadata columnMetadata) {
    int columnType = columnMetadata.getType();
    switch (columnType) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.BINARY:
        return columnMetadata.getLength();
      case Types.INTEGER:
      case Types.DECIMAL:
      case Types.BIGINT:
        return columnMetadata.getPrecision();
      case Types.DATE:
        return dateStringLength;
      case Types.TIME:
        return timeStringLength;
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
        return timestampLTZStringLength;
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
        return timestampTZStringLength;
      case Types.TIMESTAMP:
        return timestampNTZStringLength;
        // for double and boolean
        // Precision is not applicable hence return 0
      default:
        return 0;
    }
  }

  private Integer calculateDisplaySize(SnowflakeColumnMetadata columnMetadata) {
    int columnType = columnMetadata.getType();
    switch (columnType) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.BINARY:
        return columnMetadata.getLength();
      case Types.INTEGER:
      case Types.BIGINT:
        // + 1 because number can be negative, it could be -20 for number(2,0)
        return columnMetadata.getPrecision() + 1;
      case Types.DECIMAL:
        // first + 1 because number can be negative, second + 1 because it always
        // include decimal point.
        // i.e. number(2, 1) could be -1.3
        return columnMetadata.getPrecision() + 1 + 1;
      case Types.DOUBLE:
        // Hard code as 24 since the longest float
        // represented in char is
        // -2.2250738585072020E−308
        return 24;
      case Types.DATE:
        return dateStringLength;
      case Types.TIME:
        return timeStringLength;
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
        return timestampLTZStringLength;
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
        return timestampTZStringLength;
      case Types.TIMESTAMP:
        return timestampNTZStringLength;
      case Types.BOOLEAN:
        // Hard code as 5 since the longest char to represent
        // a boolean would be false, which is 5.
        return 5;
      default:
        return 25;
    }
  }

  private void calculateDateTimeStringLength() {
    SFTimestamp ts =
        SFTimestamp.fromMilliseconds(System.currentTimeMillis(), TimeZone.getDefault());
    try {
      if (timestampNTZFormatter != null) {
        String tsNTZStr =
            ResultUtil.getSFTimestampAsString(
                ts,
                Types.TIMESTAMP,
                9,
                timestampNTZFormatter,
                timestampLTZFormatter,
                timestampTZFormatter,
                session);
        timestampNTZStringLength = tsNTZStr.length();
      }
      if (timestampLTZFormatter != null) {
        String tsLTZStr =
            ResultUtil.getSFTimestampAsString(
                ts,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ,
                9,
                timestampNTZFormatter,
                timestampLTZFormatter,
                timestampTZFormatter,
                session);
        timestampLTZStringLength = tsLTZStr.length();
      }
      if (timestampTZFormatter != null) {
        String tsTZStr =
            ResultUtil.getSFTimestampAsString(
                ts,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ,
                9,
                timestampNTZFormatter,
                timestampLTZFormatter,
                timestampTZFormatter,
                session);
        timestampTZStringLength = tsTZStr.length();
      }

      SFTime time = SFTime.fromTimestamp(ts);
      if (timeFormatter != null) {
        timeStringLength = ResultUtil.getSFTimeAsString(time, 9, timeFormatter).length();
      }
      if (dateFormatter != null) {
        final Calendar calendar = Calendar.getInstance();
        calendar.set(2015, Calendar.DECEMBER, 11);
        dateStringLength =
            ResultUtil.getDateAsString(new Date(calendar.getTimeInMillis()), dateFormatter)
                .length();
      }
    } catch (SFException e) {
      logger.debug("Failed to calculate the display size. Use default one.");
    }
  }

  /**
   * get the query id
   *
   * @return query id
   */
  public String getQueryId() {
    return queryId;
  }

  /**
   * get the session
   *
   * @return session object
   */
  public SFBaseSession getSession() {
    return session;
  }

  /**
   * Get the list of column names
   *
   * @return column names in list
   */
  public List<String> getColumnNames() {
    return columnNames;
  }

  /**
   * Get the index of the column by name
   *
   * @param columnName column name
   * @return index of the column that names matches the column name
   */
  public int getColumnIndex(String columnName) {
    columnName = isResultColumnCaseInsensitive ? columnName.toUpperCase() : columnName;
    Map<String, Integer> nameToIndexMap =
        isResultColumnCaseInsensitive ? columnNameUpperCasePositionMap : columnNamePositionMap;

    if (nameToIndexMap.get(columnName) != null) {
      return nameToIndexMap.get(columnName);
    } else {
      int columnIndex =
          isResultColumnCaseInsensitive
              ? ResultUtil.listSearchCaseInsensitive(columnNames, columnName)
              : columnNames.indexOf(columnName);
      nameToIndexMap.put(columnName, columnIndex);
      return columnIndex;
    }
  }

  /**
   * Get number of columns
   *
   * @return column count
   */
  public int getColumnCount() {
    return columnCount;
  }

  public int getColumnType(int column) throws SFException {
    int internalColumnType = getInternalColumnType(column);

    int externalColumnType = internalColumnType;

    if (internalColumnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ
        || internalColumnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ) {
      externalColumnType = Types.TIMESTAMP;
    }

    return externalColumnType;
  }

  public int getInternalColumnType(int column) throws SFException {
    int columnIdx = column - 1;
    if (column < 1 || column > columnTypes.size()) {
      throw new SFException(ErrorCode.COLUMN_DOES_NOT_EXIST, column);
    }

    if (columnTypes.get(columnIdx) == null) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(ErrorCode.INTERNAL_ERROR, "Missing column type for column " + column),
              queryId,
              null);
    }

    return columnTypes.get(columnIdx);
  }

  public String getColumnTypeName(int column) throws SFException {
    if (column < 1 || column > columnTypeNames.size()) {
      throw new SFException(ErrorCode.COLUMN_DOES_NOT_EXIST, column);
    }

    if (columnTypeNames.get(column - 1) == null) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(ErrorCode.INTERNAL_ERROR, "Missing column type for column " + column),
              queryId,
              null);
    }

    return columnTypeNames.get(column - 1);
  }

  public int getScale(int column) {
    if (scales != null && scales.size() >= column) {
      return scales.get(column - 1);
    } else {
      // TODO: fix this later to use different defaults for number or timestamp
      return 9;
    }
  }

  public int getPrecision(int column) {
    if (precisions != null && precisions.size() >= column) {
      return precisions.get(column - 1);
    } else {
      // TODO: fix this later to use different defaults for number or timestamp
      return 9;
    }
  }

  public boolean isSigned(int column) {
    return (columnTypes.get(column - 1) == Types.INTEGER
        || columnTypes.get(column - 1) == Types.DECIMAL
        || columnTypes.get(column - 1) == Types.BIGINT
        || columnTypes.get(column - 1) == Types.DOUBLE);
  }

  public String getColumnLabel(int column) {
    if (columnNames != null) {
      return columnNames.get(column - 1);
    } else {
      return "C" + Integer.toString(column - 1);
    }
  }

  public String getColumnName(int column) {
    if (columnNames != null) {
      return columnNames.get(column - 1);
    } else {
      return "C" + Integer.toString(column - 1);
    }
  }

  public int isNullable(int column) {
    if (nullables != null) {
      return nullables.get(column - 1);
    } else {
      return ResultSetMetaData.columnNullableUnknown;
    }
  }

  public String getCatalogName(int column) {
    if (columnSrcDatabases == null) {
      return "";
    }
    return columnSrcDatabases.get(column - 1);
  }

  public String getSchemaName(int column) {
    if (columnSrcDatabases == null) {
      return "";
    }
    return columnSrcSchemas.get(column - 1);
  }

  public String getTableName(int column) {
    if (columnSrcDatabases == null) {
      return "T";
    }
    return columnSrcTables.get(column - 1);
  }

  public Integer getColumnDisplaySize(int column) {
    if (columnDisplaySizes == null) {
      return 25;
    }
    return columnDisplaySizes.get(column - 1);
  }
}
