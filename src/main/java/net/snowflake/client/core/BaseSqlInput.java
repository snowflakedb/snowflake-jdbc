package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;

@SnowflakeJdbcInternalApi
public abstract class BaseSqlInput implements SFSqlInput {

  protected final SFBaseSession session;
  protected final Converters converters;
  protected final List<FieldMetadata> fields;

  protected BaseSqlInput(SFBaseSession session, Converters converters, List<FieldMetadata> fields) {
    this.session = session;
    this.converters = converters;
    this.fields = fields;
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    return readTimestamp(null);
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readAsciiStream");
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBinaryStream");
  }

  @Override
  public Ref readRef() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRef");
  }

  @Override
  public Blob readBlob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBlob");
  }

  @Override
  public Clob readClob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readClob");
  }

  @Override
  public Array readArray() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readArray");
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false; // nulls are not allowed in structure types
  }

  @Override
  public URL readURL() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public NClob readNClob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNClob");
  }

  @Override
  public String readNString() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNString");
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readSQLXML");
  }

  @Override
  public RowId readRowId() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRowId");
  }

  abstract Map<String, Object> convertSqlInputToMap(SQLInput sqlInput);

  protected String convertString(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    int columnSubType = fieldMetadata.getType();
    int scale = fieldMetadata.getScale();
    return mapSFExceptionToSQLException(
        () -> converters.getStringConverter().getString(value, columnType, columnSubType, scale));
  }

  protected Boolean convertBoolean(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getBooleanConverter().getBoolean(value, columnType));
  }

  protected Short convertShort(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getShort(value, columnType));
  }

  protected Integer convertInt(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getInt(value, columnType));
  }

  protected Long convertLong(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getLong(value, columnType));
  }

  protected Float convertFloat(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getFloat(value, columnType));
  }

  protected Double convertDouble(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getDouble(value, columnType));
  }

  protected BigDecimal convertBigDecimal(Object value, FieldMetadata fieldMetadata)
      throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getBigDecimal(value, columnType));
  }

  protected byte[] convertBytes(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    int columnSubType = fieldMetadata.getType();
    int scale = fieldMetadata.getScale();
    return mapSFExceptionToSQLException(
        () -> converters.getBytesConverter().getBytes(value, columnType, columnSubType, scale));
  }
}
