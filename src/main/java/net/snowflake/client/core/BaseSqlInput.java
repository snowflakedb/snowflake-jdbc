/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.SFBaseResultSet.OBJECT_MAPPER;
import static net.snowflake.client.core.SFResultSet.logger;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
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

  protected <T> T readObjectOfType(Class<T> type, Object value, SQLInput sqlInput)
      throws SQLException {
    if (SQLData.class.isAssignableFrom(type)) {
      SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
      if (sqlInput == null) {
        return null;
      } else {
        instance.readSQL(sqlInput, null);
        return (T) instance;
      }
    } else if (Map.class.isAssignableFrom(type)) {
      Object object = value;
      if (object instanceof JsonSqlInput) {
        return (T)
            OBJECT_MAPPER.convertValue(
                ((JsonSqlInput) object).getInput(), new TypeReference<Map<String, Object>>() {});
      } else {
        return (T) ((ArrowSqlInput) object).getInput();
      }
    } else if (String.class.isAssignableFrom(type)
        || Boolean.class.isAssignableFrom(type)
        || Byte.class.isAssignableFrom(type)
        || Short.class.isAssignableFrom(type)
        || Integer.class.isAssignableFrom(type)
        || Long.class.isAssignableFrom(type)
        || Float.class.isAssignableFrom(type)
        || Double.class.isAssignableFrom(type)) {
      if (value == null) {
        return null;
      }
      return (T) value;
    } else if (Date.class.isAssignableFrom(type)) {
      return (T) readDate();
    } else if (Time.class.isAssignableFrom(type)) {
      return (T) readTime();
    } else if (Timestamp.class.isAssignableFrom(type)) {
      return (T) readTimestamp();
    } else if (BigDecimal.class.isAssignableFrom(type)) {
      return (T) readBigDecimal();
    } else {
      logger.debug(
          "Unsupported type passed to readObject(int columnIndex,Class<T> type): "
              + type.getName());
      throw new SQLException(
          "Type passed to 'getObject(int columnIndex,Class<T> type)' is unsupported. Type: "
              + type.getName());
    }
  }
}
