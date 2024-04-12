/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.io.InputStream;
import java.io.Reader;
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
}
