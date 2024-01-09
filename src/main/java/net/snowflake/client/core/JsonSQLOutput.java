package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JsonSQLOutput implements SQLOutput {

  private Vector attribs = new Vector();
  private Map map = new HashMap();
  private final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  public String getJsonString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(attribs);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

    @Override
    public void writeString(String value) throws SQLException {
        attribs.add(value);
    }

    @Override
    public void writeBoolean(boolean x) throws SQLException {

    }

    @Override
    public void writeByte(byte x) throws SQLException {

    }

    @Override
    public void writeShort(short x) throws SQLException {

    }

    @Override
    public void writeInt(int x) throws SQLException {

    }

    @Override
    public void writeLong(long x) throws SQLException {

    }

    @Override
    public void writeFloat(float x) throws SQLException {

    }

    @Override
    public void writeDouble(double x) throws SQLException {

    }

    @Override
    public void writeBigDecimal(BigDecimal x) throws SQLException {

    }

    @Override
    public void writeBytes(byte[] x) throws SQLException {

    }

    @Override
    public void writeDate(Date x) throws SQLException {

    }

    @Override
    public void writeTime(Time x) throws SQLException {

    }

    @Override
    public void writeTimestamp(Timestamp x) throws SQLException {

    }

    @Override
    public void writeCharacterStream(Reader x) throws SQLException {

    }

    @Override
    public void writeAsciiStream(InputStream x) throws SQLException {

    }

    @Override
    public void writeBinaryStream(InputStream x) throws SQLException {

    }

    @Override
    public void writeObject(SQLData x) throws SQLException {
      try {
        attribs.add(OBJECT_MAPPER.writeValueAsString(x));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void writeRef(Ref x) throws SQLException {

    }

    @Override
    public void writeBlob(Blob x) throws SQLException {

    }

    @Override
    public void writeClob(Clob x) throws SQLException {

    }

    @Override
    public void writeStruct(Struct x) throws SQLException {

    }

    @Override
    public void writeArray(Array x) throws SQLException {

    }

    @Override
    public void writeURL(URL x) throws SQLException {

    }

    @Override
    public void writeNString(String x) throws SQLException {

    }

    @Override
    public void writeNClob(NClob x) throws SQLException {

    }

    @Override
    public void writeRowId(RowId x) throws SQLException {

    }

    @Override
    public void writeSQLXML(SQLXML x) throws SQLException {

    }
}

