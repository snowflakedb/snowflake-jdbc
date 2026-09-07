package net.snowflake.client.internal.core;

import static net.snowflake.client.TestUtil.expectSnowflakeLoggedFeatureNotSupportedException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Timestamp;
import java.util.HashMap;
import net.snowflake.client.internal.jdbc.BindingParameterMetadata;
import org.junit.jupiter.api.Test;

public class SQLInputOutputTest {

  @Test
  public void testBaseSQLUnSupportedException() {
    BaseSqlInput sqlInput = new ArrowSqlInput(null, null, null, null);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readCharacterStream);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readAsciiStream);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readBinaryStream);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readRef);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readBlob);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readClob);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readArray);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readURL);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readNClob);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readNString);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readSQLXML);
    expectSnowflakeLoggedFeatureNotSupportedException(sqlInput::readRowId);
  }

  @Test
  public void testJsonSqlOutPutUnSupportedTest() {
    JsonSqlOutput sqloutput = new JsonSqlOutput(mock(SQLData.class), mock(SFBaseSession.class));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeRef(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeBlob(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeClob(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeStruct(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeArray(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeURL(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeNString(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeNClob(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeRowId(null));
    expectSnowflakeLoggedFeatureNotSupportedException(() -> sqloutput.writeSQLXML(null));
  }

  @Test
  public void testWriteObjectNullNestedDoesNotThrow() throws SQLException {
    SFSession session = createSession();
    NestedNullHolder holder = new NestedNullHolder();
    JsonSqlOutput output = new JsonSqlOutput(holder, session);
    holder.writeSQL(output);

    assertNullField(output, "nested");
  }

  @Test
  public void testWriteBigDecimalNullDoesNotThrow() throws SQLException {
    SFSession session = createSession();
    BigDecimalNullHolder holder = new BigDecimalNullHolder();
    JsonSqlOutput output = new JsonSqlOutput(holder, session);
    holder.writeSQL(output);

    assertNullField(output, "amount");
  }

  @Test
  public void testWriteBytesNullDoesNotThrow() throws SQLException {
    SFSession session = createSession();
    BytesNullHolder holder = new BytesNullHolder();
    JsonSqlOutput output = new JsonSqlOutput(holder, session);
    holder.writeSQL(output);

    assertNullField(output, "payload");
  }

  @Test
  public void testWriteTimestampNullDoesNotThrow() throws SQLException {
    SFSession session = createSession();
    TimestampNullHolder holder = new TimestampNullHolder();
    JsonSqlOutput output = new JsonSqlOutput(holder, session);
    holder.writeSQL(output);

    assertNullField(output, "createdAt");
  }

  @Test
  public void testWriteDateNullDoesNotThrow() throws SQLException {
    SFSession session = createSession();
    DateNullHolder holder = new DateNullHolder();
    JsonSqlOutput output = new JsonSqlOutput(holder, session);
    holder.writeSQL(output);

    assertNullField(output, "eventDate");
  }

  private static SFSession createSession() {
    SFSession session = new SFSession();
    session.setCommonParameters(new HashMap<>());
    return session;
  }

  private static void assertNullField(JsonSqlOutput output, String fieldName) {
    assertTrue(output.getJsonObject().containsKey(fieldName));
    assertNull(output.getJsonObject().get(fieldName));
    BindingParameterMetadata fieldSchema = output.getSchema().getFields().get(0);
    assertEquals(fieldName, fieldSchema.getName());
    assertEquals("object", fieldSchema.getType());
    assertTrue(fieldSchema.getFields().isEmpty());
  }

  private static class NestedNullHolder implements SQLData {
    private SQLData nested;

    @Override
    public String getSQLTypeName() {
      return null;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) {}

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeObject(nested);
    }
  }

  private static class BigDecimalNullHolder implements SQLData {
    private BigDecimal amount;

    @Override
    public String getSQLTypeName() {
      return null;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) {}

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeBigDecimal(amount);
    }
  }

  private static class BytesNullHolder implements SQLData {
    private byte[] payload;

    @Override
    public String getSQLTypeName() {
      return null;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) {}

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeBytes(payload);
    }
  }

  private static class TimestampNullHolder implements SQLData {
    private Timestamp createdAt;

    @Override
    public String getSQLTypeName() {
      return null;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) {}

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeTimestamp(createdAt);
    }
  }

  private static class DateNullHolder implements SQLData {
    private Date eventDate;

    @Override
    public String getSQLTypeName() {
      return null;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) {}

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeDate(eventDate);
    }
  }
}
