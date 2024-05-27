package net.snowflake.client.core.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import net.snowflake.client.core.BaseSqlInput;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import org.junit.Test;

public class BaseSqlInputTest {

  public class MockSqlInput extends BaseSqlInput {
    MockSqlInput() {
      super(null, null, null);
    }

    public Map<String, Object> convertSqlInputToMap(SQLInput sqlInput) {
      return null;
    }

    @Override
    public Timestamp readTimestamp() throws SQLException {
      return super.readTimestamp();
    }

    @Override
    public Timestamp readTimestamp(TimeZone tz) throws SQLException {
      return null;
    }

    @Override
    public <T> T readObject(Class<T> type, TimeZone tz) throws SQLException {
      return null;
    }

    @Override
    public <T> List<T> readList(Class<T> type) throws SQLException {
      return null;
    }

    @Override
    public <T> Map<String, T> readMap(Class<T> type) throws SQLException {
      return null;
    }

    @Override
    public <T> T[] readArray(Class<T> type) throws SQLException {
      return null;
    }

    @Override
    public String readString() throws SQLException {
      return null;
    }

    @Override
    public boolean readBoolean() throws SQLException {
      return false;
    }

    @Override
    public byte readByte() throws SQLException {
      return 0;
    }

    @Override
    public short readShort() throws SQLException {
      return 0;
    }

    @Override
    public int readInt() throws SQLException {
      return 0;
    }

    @Override
    public long readLong() throws SQLException {
      return 0;
    }

    @Override
    public float readFloat() throws SQLException {
      return 0;
    }

    @Override
    public double readDouble() throws SQLException {
      return 0;
    }

    @Override
    public BigDecimal readBigDecimal() throws SQLException {
      return null;
    }

    @Override
    public byte[] readBytes() throws SQLException {
      return new byte[0];
    }

    @Override
    public Date readDate() throws SQLException {
      return null;
    }

    @Override
    public Time readTime() throws SQLException {
      return null;
    }

    @Override
    public Object readObject() throws SQLException {
      return null;
    }
  }

  @Test
  public void testBaseSqlInput() throws SQLException {
    BaseSqlInput sqlInput = new MockSqlInput();
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readCharacterStream();
          return null;
        }),
        "readCharacterStream");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readAsciiStream();
          return null;
        }),
        "readAsciiStream");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readBinaryStream();
          return null;
        }),
        "readBinaryStream");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readRef();
          return null;
        }),
        "readRef");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readBlob();
          return null;
        }),
        "readBlob");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readClob();
          return null;
        }),
        "readClob");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readArray();
          return null;
        }),
        "readArray");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readURL();
          return null;
        }),
        "readCharacterStream");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readNClob();
          return null;
        }),
        "readNClob");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readNString();
          return null;
        }),
        "readNString");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readSQLXML();
          return null;
        }),
        "readSQLXML");
    expectedSnowflakeLoggedFeatureNotSupportedException(
        (() -> {
          sqlInput.readRowId();
          return null;
        }),
        "readRowId");
    assertEquals(sqlInput.wasNull(), false);
  }

  protected void expectedSnowflakeLoggedFeatureNotSupportedException(
      Callable<Void> f, String message) {
    try {
      f.call();
      fail("must raise exception");
    } catch (SnowflakeLoggedFeatureNotSupportedException ex) {
      assertEquals(ex.getMessage(), message);
    } catch (Exception sqlEx) {
      fail("Unexpected exception");
    }
  }
}
