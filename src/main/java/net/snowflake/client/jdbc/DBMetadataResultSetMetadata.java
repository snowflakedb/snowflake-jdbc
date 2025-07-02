package net.snowflake.client.jdbc;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * For function call getTables/getSchemas, we returned resultset. We stored these resultSetMetadata
 * here
 */
public enum DBMetadataResultSetMetadata {
  GET_CATALOGS(
      Collections.singletonList("TABLE_CAT"),
      Collections.singletonList("TEXT"),
      Collections.singletonList(Types.VARCHAR)),

  GET_SCHEMAS(
      Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
      Arrays.asList("TEXT", "TEXT"),
      Arrays.asList(Types.VARCHAR, Types.VARCHAR)),

  GET_TABLES(
      Arrays.asList(
          "TABLE_CAT",
          "TABLE_SCHEM",
          "TABLE_NAME",
          "TABLE_TYPE",
          "REMARKS",
          "TYPE_CAT",
          "TYPE_SCHEM",
          "TYPE_NAME",
          "SELF_REFERENCING_COL_NAME",
          "REF_GENERATION"),
      Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR)),

  GET_COLUMNS(
      Arrays.asList(
          "TABLE_CAT",
          "TABLE_SCHEM",
          "TABLE_NAME",
          "COLUMN_NAME",
          "DATA_TYPE",
          "TYPE_NAME",
          "COLUMN_SIZE",
          "BUFFER_LENGTH",
          "DECIMAL_DIGITS",
          "NUM_PREC_RADIX",
          "NULLABLE",
          "REMARKS",
          "COLUMN_DEF",
          "SQL_DATA_TYPE",
          "SQL_DATETIME_SUB",
          "CHAR_OCTET_LENGTH",
          "ORDINAL_POSITION",
          "IS_NULLABLE",
          "SCOPE_CATALOG",
          "SCOPE_SCHEMA",
          "SCOPE_TABLE",
          "SOURCE_DATA_TYPE",
          "IS_AUTOINCREMENT",
          "IS_GENERATEDCOLUMN"),
      Arrays.asList(
          "TEXT", "TEXT", "TEXT", "TEXT", "INTEGER", "TEXT", "INTEGER", "INTEGER", "INTEGER",
          "INTEGER", "INTEGER", "TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "TEXT",
          "TEXT", "TEXT", "TEXT", "SHORT", "TEXT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.INTEGER,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.VARCHAR,
          Types.VARCHAR)),

  GET_COLUMNS_EXTENDED_SET(
      GET_COLUMNS,
      Collections.singletonList("BASE_TYPE"),
      Collections.singletonList("TEXT"),
      Collections.singletonList(Types.VARCHAR)),

  GET_PRIMARY_KEYS(
      Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
      Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "INTEGER", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.VARCHAR)),

  GET_FOREIGN_KEYS(
      Arrays.asList(
          "PKTABLE_CAT",
          "PKTABLE_SCHEM",
          "PKTABLE_NAME",
          "PKCOLUMN_NAME",
          "FKTABLE_CAT",
          "FKTABLE_SCHEM",
          "FKTABLE_NAME",
          "FKCOLUMN_NAME",
          "KEY_SEQ",
          "UPDATE_RULE",
          "DELETE_RULE",
          "FK_NAME",
          "PK_NAME",
          "DEFERRABILITY"),
      Arrays.asList(
          "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "SHORT", "SHORT",
          "TEXT", "TEXT", "SHORT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.SMALLINT,
          Types.SMALLINT,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT)),

  GET_FUNCTIONS(
      Arrays.asList(
          "FUNCTION_CAT",
          "FUNCTION_SCHEM",
          "FUNCTION_NAME",
          "REMARKS",
          "FUNCTION_TYPE",
          "SPECIFIC_NAME"),
      Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.VARCHAR)),

  GET_FUNCTION_COLUMNS(
      Arrays.asList(
          "FUNCTION_CAT",
          "FUNCTION_SCHEM",
          "FUNCTION_NAME",
          "COLUMN_NAME",
          "COLUMN_TYPE",
          "DATA_TYPE",
          "TYPE_NAME",
          "PRECISION",
          "LENGTH",
          "SCALE",
          "RADIX",
          "NULLABLE",
          "REMARKS",
          "CHAR_OCTET_LENGTH",
          "ORDINAL_POSITION",
          "IS_NULLABLE",
          "SPECIFIC_NAME"),
      Arrays.asList(
          "TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "INTEGER", "TEXT", "INTEGER", "INTEGER", "SHORT",
          "SHORT", "SHORT", "TEXT", "INTEGER", "INTEGER", "TEXT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.INTEGER,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.SMALLINT,
          Types.SMALLINT,
          Types.SMALLINT,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.VARCHAR,
          Types.VARCHAR)),

  GET_PROCEDURES(
      Arrays.asList(
          "PROCEDURE_CAT",
          "PROCEDURE_SCHEM",
          "PROCEDURE_NAME",
          "REMARKS",
          "PROCEDURE_TYPE",
          "SPECIFIC_NAME"),
      Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.VARCHAR)),
  GET_PROCEDURE_COLUMNS(
      Arrays.asList(
          "PROCEDURE_CAT",
          "PROCEDURE_SCHEM",
          "PROCEDURE_NAME",
          "COLUMN_NAME",
          "COLUMN_TYPE",
          "DATA_TYPE",
          "TYPE_NAME",
          "PRECISION",
          "LENGTH",
          "SCALE",
          "RADIX",
          "NULLABLE",
          "REMARKS",
          "COLUMN_DEF",
          "SQL_DATA_TYPE",
          "SQL_DATETIME_SUB",
          "CHAR_OCTET_LENGTH",
          "ORDINAL_POSITION",
          "IS_NULLABLE",
          "SPECIFIC_NAME"),
      Arrays.asList(
          "TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "INTEGER", "TEXT", "INTEGER", "INTEGER", "SHORT",
          "SHORT", "SHORT", "TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "TEXT",
          "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.SMALLINT,
          Types.INTEGER,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.SMALLINT,
          Types.SMALLINT,
          Types.SMALLINT,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.VARCHAR,
          Types.VARCHAR)),
  GET_TABLE_PRIVILEGES(
      Arrays.asList(
          "TABLE_CAT",
          "TABLE_SCHEM",
          "TABLE_NAME",
          "GRANTOR",
          "GRANTEE",
          "PRIVILEGE",
          "IS_GRANTABLE"),
      Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR)),

  GET_STREAMS(
      Arrays.asList(
          "STREAM_NAME",
          "DATABASE_NAME",
          "SCHEMA_NAME",
          "OWNER",
          "COMMENT",
          "TABLE_NAME",
          "SOURCE_TYPE",
          "BASE_TABLES",
          "TYPE",
          "STALE",
          "MODE"),
      Arrays.asList(
          "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"),
      Arrays.asList(
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.VARCHAR)),
  ;

  private List<String> columnNames;

  private List<String> columnTypeNames;

  private List<Integer> columnTypes;

  DBMetadataResultSetMetadata(
      List<String> columnNames, List<String> columnTypeNames, List<Integer> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypeNames = columnTypeNames;
    this.columnTypes = columnTypes;
  }

  DBMetadataResultSetMetadata(
      DBMetadataResultSetMetadata base,
      List<String> additionalColumnNames,
      List<String> additionalColumnTypeNames,
      List<Integer> additionalColumnTypes) {
    this.columnNames = new ArrayList<>(base.getColumnNames());
    this.columnTypeNames = new ArrayList<>(base.getColumnTypeNames());
    this.columnTypes = new ArrayList<>(base.getColumnTypes());
    columnNames.addAll(additionalColumnNames);
    columnTypeNames.addAll(additionalColumnTypeNames);
    columnTypes.addAll(additionalColumnTypes);
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<String> getColumnTypeNames() {
    return columnTypeNames;
  }

  public List<Integer> getColumnTypes() {
    return columnTypes;
  }
}
