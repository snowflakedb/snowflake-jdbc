package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.sql.SQLData;
import java.sql.SQLException;
import net.snowflake.common.core.SqlState;
import org.junit.Test;

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

  private interface MethodRaisesSQLException {
    void run() throws SQLException;
  }

  private void expectSnowflakeLoggedFeatureNotSupportedException(MethodRaisesSQLException f) {
    try {
      f.run();
      fail("must raise exception");
    } catch (SQLException ex) {
      assertEquals(SqlState.FEATURE_NOT_SUPPORTED, "0A000");
    }
  }
}
