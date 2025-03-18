package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.core.SFException;

public class BaseJDBCTest extends AbstractDriverIT {
  // Test UUID unique per session
  static final String TEST_UUID = UUID.randomUUID().toString();

  protected interface MethodRaisesSQLException {
    void run() throws SQLException;
  }

  protected interface MethodRaisesSFException {
    void run() throws SFException;
  }

  protected interface MethodRaisesSQLClientInfoException {
    void run() throws SQLClientInfoException;
  }

  protected void expectConnectionAlreadyClosedException(MethodRaisesSQLException f) {
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> {
              f.run();
            });
    assertEquals((int) ErrorCode.CONNECTION_CLOSED.getMessageCode(), ex.getErrorCode());
  }

  protected void expectStatementAlreadyClosedException(MethodRaisesSQLException f) {
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> {
              f.run();
            });
    assertEquals((int) ErrorCode.STATEMENT_CLOSED.getMessageCode(), ex.getErrorCode());
  }

  protected void expectResultSetAlreadyClosedException(MethodRaisesSQLException f) {
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> {
              f.run();
            });
    assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
  }

  protected void expectFeatureNotSupportedException(MethodRaisesSQLException f) {
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> {
              f.run();
            });
    assertTrue(ex instanceof SQLFeatureNotSupportedException);
  }

  protected void expectSQLClientInfoException(MethodRaisesSQLClientInfoException f) {
    SQLClientInfoException ex =
        assertThrows(
            SQLClientInfoException.class,
            () -> {
              f.run();
            });
  }

  int getSizeOfResultSet(ResultSet rs) throws SQLException {
    int count = 0;
    while (rs.next()) {
      count++;
    }
    return count;
  }

  List<String> getInfoBySQL(String sqlCmd) throws SQLException {
    Connection con = getConnection();
    Statement st = con.createStatement();
    List<String> result = new ArrayList<>();
    ResultSet rs = st.executeQuery(sqlCmd);
    while (rs.next()) {
      result.add(rs.getString(1));
    }
    return result;
  }

  /**
   * Parses a datetime string in the format YYYY-MM-DD HH24:MI:SS.S... +TZH:TZM and creates
   * ZonedDateTime object.
   *
   * @param dateTimeString a datetime string
   * @return a ZonedDateTime object.
   */
  ZonedDateTime parseTimestampTZ(String dateTimeString) {
    String[] dts = dateTimeString.split("\\s");
    return ZonedDateTime.parse(
        (dts[0] + "T" + dts[1] + dts[2].substring(0, 3) + ":" + dts[2].substring(3)));
  }

  /**
   * Parses a datetime string in the format YYYY-MM-DD HH24:MI:SS.S... and creates LocalDateTime
   * object.
   *
   * @param dateTimeString a datetime string
   * @return a LocalDateTime object.
   */
  LocalDateTime parseTimestampNTZ(String dateTimeString) {
    String[] dts = dateTimeString.split("\\s");
    return LocalDateTime.parse(dts[0] + "T" + dts[1]);
  }

  class FakeRef implements Ref {

    @Override
    public String getBaseTypeName() throws SQLException {
      return null;
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
      return null;
    }

    @Override
    public Object getObject() throws SQLException {
      return null;
    }

    @Override
    public void setObject(Object value) throws SQLException {}
  }

  class FakeBlob implements Blob {
    @Override
    public long length() throws SQLException {
      return 0;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
      return new byte[0];
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
      return null;
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
      return 0;
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
      return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
      return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
      return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
      return null;
    }

    @Override
    public void truncate(long len) throws SQLException {}

    @Override
    public void free() throws SQLException {}

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
      return null;
    }
  }

  class FakeArray implements Array {
    @Override
    public String getBaseTypeName() throws SQLException {
      return null;
    }

    @Override
    public int getBaseType() throws SQLException {
      return 0;
    }

    @Override
    public Object getArray() throws SQLException {
      return null;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
      return null;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
      return null;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
      return null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
      return null;
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
      return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
      return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
        throws SQLException {
      return null;
    }

    @Override
    public void free() throws SQLException {}
  }

  class FakeRowId implements RowId {
    @Override
    public byte[] getBytes() {
      return new byte[0];
    }
  }

  class FakeNClob implements NClob {
    @Override
    public long length() throws SQLException {
      return 0;
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
      return null;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
      return null;
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
      return null;
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
      return 0;
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
      return 0;
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
      return 0;
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
      return 0;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
      return null;
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
      return null;
    }

    @Override
    public void truncate(long len) throws SQLException {}

    @Override
    public void free() throws SQLException {}

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
      return null;
    }
  }

  class FakeSQLXML implements SQLXML {
    @Override
    public void free() throws SQLException {}

    @Override
    public InputStream getBinaryStream() throws SQLException {
      return null;
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
      return null;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
      return null;
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
      return null;
    }

    @Override
    public String getString() throws SQLException {
      return null;
    }

    @Override
    public void setString(String value) throws SQLException {}

    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
      return null;
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
      return null;
    }
  }

  class FakeReader extends Reader {
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {}
  }

  class FakeInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      return 0;
    }
  }

  class FakeCalendar extends Calendar {

    @Override
    protected void computeTime() {}

    @Override
    protected void computeFields() {}

    @Override
    public void add(int field, int amount) {}

    @Override
    public void roll(int field, boolean up) {}

    @Override
    public int getMinimum(int field) {
      return 0;
    }

    @Override
    public int getMaximum(int field) {
      return 0;
    }

    @Override
    public int getGreatestMinimum(int field) {
      return 0;
    }

    @Override
    public int getLeastMaximum(int field) {
      return 0;
    }
  }
}
