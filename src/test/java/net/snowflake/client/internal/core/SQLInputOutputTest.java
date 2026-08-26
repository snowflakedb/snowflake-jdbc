package net.snowflake.client.internal.core;

import static net.snowflake.client.TestUtil.expectSnowflakeLoggedFeatureNotSupportedException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
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
    SFSession session = new SFSession();
    session.setCommonParameters(new HashMap<>());

    NestedNullHolder holder = new NestedNullHolder();
    JsonSqlOutput output = new JsonSqlOutput(holder, session);
    holder.writeSQL(output);

    assertTrue(output.getJsonObject().containsKey("nested"));
    assertNull(output.getJsonObject().get("nested"));
    BindingParameterMetadata nestedSchema = output.getSchema().getFields().get(0);
    assertEquals("nested", nestedSchema.getName());
    assertEquals("object", nestedSchema.getType());
    assertTrue(nestedSchema.getFields().isEmpty());
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
}
